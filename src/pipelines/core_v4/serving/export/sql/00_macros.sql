-- Shared macros + small tables for the final export. Everything here is TEMP (the source DB is opened read-only).
-- Decisions: see ../../SERVING_DESIGN.md (D-numbers in the comments).

-- ---- D14: ECB euro reference rates of 2026-09-18, units of currency per 1 EUR (spot-checked against the raw ECB xml on 2026-09-20).
-- HRK (fixed 7.53450, Croatia joined the euro 2023) and BGN (fixed peg 1.95583) are not in the ECB list.
-- A currency not in this table -> funded_amount_eur NULL (never guess).
CREATE OR REPLACE TEMP TABLE fx AS
SELECT * FROM (VALUES
  ('EUR', 1.0), ('USD', 1.1460), ('JPY', 180.94), ('CZK', 24.339), ('DKK', 7.4754), ('GBP', 0.85880), ('HUF', 364.28),
  ('PLN', 4.3635), ('RON', 5.2647), ('SEK', 11.2915), ('CHF', 0.9462), ('ISK', 139.40), ('NOK', 10.8095), ('TRY', 55.9077),
  ('AUD', 1.6095), ('BRL', 5.8857), ('CAD', 1.6056), ('CNY', 7.6755), ('HKD', 8.9903), ('IDR', 20424.81), ('ILS', 3.4812),
  ('INR', 109.8755), ('KRW', 1590.76), ('MXN', 19.6855), ('MYR', 4.6763), ('NZD', 2.0068), ('PHP', 71.972), ('SGD', 1.4651),
  ('THB', 38.225), ('ZAR', 18.6482), ('HRK', 7.53450), ('BGN', 1.95583)) AS t(currency, units_per_eur);

-- D21: currency normalisation. NULL currency of SNSF (Swiss, 92k projects with an amount but no currency) -> CHF;
-- '$' is USD except for NHMRC (Australian) -> AUD; everything else as is.
CREATE OR REPLACE TEMP MACRO hm_cur(cur, funders) AS
  CASE WHEN cur IS NULL AND list_contains(coalesce(funders, []), 'SNSF') THEN 'CHF'
       WHEN cur = '$' AND list_contains(coalesce(funders, []), 'NHMRC') THEN 'AUD'
       WHEN cur = '$' THEN 'USD'
       ELSE cur END;

-- D27: text cleaning (`hm_clean`), tested in test_usecases.py (`hm_clean_unit`) and verify_clean.py on the real data.
-- The source data (OpenAIRE / Crossref / Cordis) carries HTML: `&amp;`, double-escaped `&amp;amp;` / `&amp;lt;`, numeric entities, and real markup
-- like `CO<sub>2</sub>`. Order of operations: (1) decode entities twice (so `&amp;lt;` -> `&lt;` -> `<`), everything except `&amp;` first and `&amp;`
-- LAST in each pass; (2) strip a WHITELIST of real tags: inline tags -> '' (`CO<sub>2</sub>` -> `CO2`), block tags -> ' ' (`a<br>b` -> `a b`);
-- unknown `<...>` and plain comparisons (`p < 0.05 and x > 3`) stay untouched; (3) collapse whitespace, empty -> NULL. Idempotent on clean text.
-- The regexes only run on values that contain `&` / `<` (cheap on the 50M works).
-- MathML (`<mi>`, `<mo>`, `<math>`, prefixed `<a:mi>`, `<inline-formula>`, `<tex-math>` ...) is 250k+ of the real titles: inline tags too.
-- Unknown pseudo-tags like the structured-abstract markers `<Background >` / `<Objectives>` (~600 project summaries) stay on purpose.
-- Limits (deliberate): a whitelisted tag name directly after `<` followed by `>` (or by `attr=...>`) is a tag, e.g. `a<b>c` loses `<b>`; numeric
-- entities are decoded generically (control characters and invalid code points are dropped), named entities cover the HTML 4 set + `&apos;`.
CREATE OR REPLACE TEMP MACRO hm_cp(n) AS
  CASE WHEN n IS NULL OR n = 0 OR n > 1114111 OR n BETWEEN 55296 AND 57343 THEN ''
       WHEN n IN (9, 10, 13, 160, 8192, 8193, 8194, 8195, 8196, 8197, 8198, 8199, 8200, 8201, 8202, 8232, 8233, 8239, 12288) THEN ' '
       WHEN n < 32 OR n = 127 OR n BETWEEN 128 AND 159 OR n IN (173, 8203, 8204, 8205, 8206, 8207, 8288, 65279) THEN ''
       ELSE chr(n::INTEGER) END;

CREATE OR REPLACE TEMP MACRO hm_ent(e) AS
  CASE WHEN e[2] = '#' THEN
         hm_cp(try_cast(CASE WHEN lower(e[3]) = 'x' THEN '0x' || substr(e, 4, length(e) - 4) ELSE substr(e, 3, length(e) - 3) END AS BIGINT))
       ELSE coalesce(MAP {
  '&AElig;': 'Æ', '&Aacute;': 'Á', '&Acirc;': 'Â', '&Agrave;': 'À', '&Alpha;': 'Α', '&Aring;': 'Å', '&Atilde;': 'Ã', '&Auml;': 'Ä', '&Beta;': 'Β',
  '&Ccedil;': 'Ç', '&Chi;': 'Χ', '&Dagger;': '‡', '&Delta;': 'Δ', '&ETH;': 'Ð', '&Eacute;': 'É', '&Ecirc;': 'Ê', '&Egrave;': 'È', '&Epsilon;': 'Ε',
  '&Eta;': 'Η', '&Euml;': 'Ë', '&Gamma;': 'Γ', '&Iacute;': 'Í', '&Icirc;': 'Î', '&Igrave;': 'Ì', '&Iota;': 'Ι', '&Iuml;': 'Ï', '&Kappa;': 'Κ',
  '&Lambda;': 'Λ', '&Mu;': 'Μ', '&Ntilde;': 'Ñ', '&Nu;': 'Ν', '&OElig;': 'Œ', '&Oacute;': 'Ó', '&Ocirc;': 'Ô', '&Ograve;': 'Ò', '&Omega;': 'Ω',
  '&Omicron;': 'Ο', '&Oslash;': 'Ø', '&Otilde;': 'Õ', '&Ouml;': 'Ö', '&Phi;': 'Φ', '&Pi;': 'Π', '&Prime;': '″', '&Psi;': 'Ψ', '&Rho;': 'Ρ',
  '&Scaron;': 'Š', '&Sigma;': 'Σ', '&THORN;': 'Þ', '&Tau;': 'Τ', '&Theta;': 'Θ', '&Uacute;': 'Ú', '&Ucirc;': 'Û', '&Ugrave;': 'Ù', '&Upsilon;': 'Υ',
  '&Uuml;': 'Ü', '&Xi;': 'Ξ', '&Yacute;': 'Ý', '&Yuml;': 'Ÿ', '&Zeta;': 'Ζ', '&aacute;': 'á', '&acirc;': 'â', '&acute;': '´', '&aelig;': 'æ',
  '&agrave;': 'à', '&alefsym;': 'ℵ', '&alpha;': 'α', '&and;': '∧', '&ang;': '∠', '&aring;': 'å', '&asymp;': '≈', '&atilde;': 'ã', '&auml;': 'ä',
  '&bdquo;': '„', '&beta;': 'β', '&brvbar;': '¦', '&bull;': '•', '&cap;': '∩', '&ccedil;': 'ç', '&cedil;': '¸', '&cent;': '¢', '&chi;': 'χ',
  '&circ;': 'ˆ', '&clubs;': '♣', '&cong;': '≅', '&copy;': '©', '&crarr;': '↵', '&cup;': '∪', '&curren;': '¤', '&dArr;': '⇓', '&dagger;': '†',
  '&darr;': '↓', '&deg;': '°', '&delta;': 'δ', '&diams;': '♦', '&divide;': '÷', '&eacute;': 'é', '&ecirc;': 'ê', '&egrave;': 'è', '&empty;': '∅',
  '&emsp;': ' ', '&ensp;': ' ', '&epsilon;': 'ε', '&equiv;': '≡', '&eta;': 'η', '&eth;': 'ð', '&euml;': 'ë', '&euro;': '€', '&exist;': '∃',
  '&fnof;': 'ƒ', '&forall;': '∀', '&frac12;': '½', '&frac14;': '¼', '&frac34;': '¾', '&frasl;': '⁄', '&gamma;': 'γ', '&ge;': '≥', '&gt;': '>',
  '&hArr;': '⇔', '&harr;': '↔', '&hearts;': '♥', '&hellip;': '…', '&iacute;': 'í', '&icirc;': 'î', '&iexcl;': '¡', '&igrave;': 'ì', '&image;': 'ℑ',
  '&infin;': '∞', '&int;': '∫', '&iota;': 'ι', '&iquest;': '¿', '&isin;': '∈', '&iuml;': 'ï', '&kappa;': 'κ', '&lArr;': '⇐', '&lambda;': 'λ',
  '&lang;': '〈', '&laquo;': '«', '&larr;': '←', '&lceil;': '⌈', '&ldquo;': '“', '&le;': '≤', '&lfloor;': '⌊', '&lowast;': '∗', '&loz;': '◊',
  '&lrm;': '', '&lsaquo;': '‹', '&lsquo;': '‘', '&lt;': '<', '&macr;': '¯', '&mdash;': '—', '&micro;': 'µ', '&middot;': '·', '&minus;': '−',
  '&mu;': 'μ', '&nabla;': '∇', '&nbsp;': ' ', '&ndash;': '–', '&ne;': '≠', '&ni;': '∋', '&not;': '¬', '&notin;': '∉', '&nsub;': '⊄', '&ntilde;': 'ñ',
  '&nu;': 'ν', '&oacute;': 'ó', '&ocirc;': 'ô', '&oelig;': 'œ', '&ograve;': 'ò', '&oline;': '‾', '&omega;': 'ω', '&omicron;': 'ο', '&oplus;': '⊕',
  '&or;': '∨', '&ordf;': 'ª', '&ordm;': 'º', '&oslash;': 'ø', '&otilde;': 'õ', '&otimes;': '⊗', '&ouml;': 'ö', '&para;': '¶', '&part;': '∂',
  '&permil;': '‰', '&perp;': '⊥', '&phi;': 'φ', '&pi;': 'π', '&piv;': 'ϖ', '&plusmn;': '±', '&pound;': '£', '&prime;': '′', '&prod;': '∏',
  '&prop;': '∝', '&psi;': 'ψ', '&quot;': '"', '&rArr;': '⇒', '&radic;': '√', '&rang;': '〉', '&raquo;': '»', '&rarr;': '→', '&rceil;': '⌉',
  '&rdquo;': '”', '&real;': 'ℜ', '&reg;': '®', '&rfloor;': '⌋', '&rho;': 'ρ', '&rlm;': '', '&rsaquo;': '›', '&rsquo;': '’', '&sbquo;': '‚',
  '&scaron;': 'š', '&sdot;': '⋅', '&sect;': '§', '&shy;': '', '&sigma;': 'σ', '&sigmaf;': 'ς', '&sim;': '∼', '&spades;': '♠', '&sub;': '⊂',
  '&sube;': '⊆', '&sum;': '∑', '&sup;': '⊃', '&sup1;': '¹', '&sup2;': '²', '&sup3;': '³', '&supe;': '⊇', '&szlig;': 'ß', '&tau;': 'τ', '&there4;': '∴',
  '&theta;': 'θ', '&thetasym;': 'ϑ', '&thinsp;': ' ', '&thorn;': 'þ', '&tilde;': '˜', '&times;': '×', '&trade;': '™', '&uArr;': '⇑', '&uacute;': 'ú',
  '&uarr;': '↑', '&ucirc;': 'û', '&ugrave;': 'ù', '&uml;': '¨', '&upsih;': 'ϒ', '&upsilon;': 'υ', '&uuml;': 'ü', '&weierp;': '℘', '&xi;': 'ξ',
  '&yacute;': 'ý', '&yen;': '¥', '&yuml;': 'ÿ', '&zeta;': 'ζ', '&zwj;': '', '&zwnj;': '', '&apos;': '''', '&Abreve;': 'Ă', '&Amacr;': 'Ā',
  '&Aogon;': 'Ą', '&Cacute;': 'Ć', '&Ccaron;': 'Č', '&Ccirc;': 'Ĉ', '&Cdot;': 'Ċ', '&Dcaron;': 'Ď', '&Dstrok;': 'Đ', '&ENG;': 'Ŋ', '&Ecaron;': 'Ě',
  '&Edot;': 'Ė', '&Emacr;': 'Ē', '&Eogon;': 'Ę', '&Gbreve;': 'Ğ', '&Gcedil;': 'Ģ', '&Gcirc;': 'Ĝ', '&Gdot;': 'Ġ', '&Hcirc;': 'Ĥ', '&Hstrok;': 'Ħ',
  '&IJlig;': 'Ĳ', '&Idot;': 'İ', '&Imacr;': 'Ī', '&Iogon;': 'Į', '&Itilde;': 'Ĩ', '&Jcirc;': 'Ĵ', '&Kcedil;': 'Ķ', '&Lacute;': 'Ĺ', '&Lcaron;': 'Ľ',
  '&Lcedil;': 'Ļ', '&Lmidot;': 'Ŀ', '&Lstrok;': 'Ł', '&Nacute;': 'Ń', '&Ncaron;': 'Ň', '&Ncedil;': 'Ņ', '&Odblac;': 'Ő', '&Omacr;': 'Ō',
  '&Racute;': 'Ŕ', '&Rcaron;': 'Ř', '&Rcedil;': 'Ŗ', '&Sacute;': 'Ś', '&Scedil;': 'Ş', '&Scirc;': 'Ŝ', '&Tcaron;': 'Ť', '&Tcedil;': 'Ţ',
  '&Tstrok;': 'Ŧ', '&Ubreve;': 'Ŭ', '&Udblac;': 'Ű', '&Umacr;': 'Ū', '&Uogon;': 'Ų', '&Uring;': 'Ů', '&Utilde;': 'Ũ', '&Wcirc;': 'Ŵ', '&Ycirc;': 'Ŷ',
  '&Zacute;': 'Ź', '&Zcaron;': 'Ž', '&Zdot;': 'Ż', '&abreve;': 'ă', '&amacr;': 'ā', '&aogon;': 'ą', '&cacute;': 'ć', '&ccaron;': 'č', '&ccirc;': 'ĉ',
  '&cdot;': 'ċ', '&dcaron;': 'ď', '&dstrok;': 'đ', '&ecaron;': 'ě', '&edot;': 'ė', '&emacr;': 'ē', '&eng;': 'ŋ', '&eogon;': 'ę', '&gbreve;': 'ğ',
  '&gcirc;': 'ĝ', '&gdot;': 'ġ', '&hcirc;': 'ĥ', '&hstrok;': 'ħ', '&ijlig;': 'ĳ', '&imacr;': 'ī', '&imath;': 'ı', '&inodot;': 'ı', '&iogon;': 'į',
  '&itilde;': 'ĩ', '&jcirc;': 'ĵ', '&kcedil;': 'ķ', '&kgreen;': 'ĸ', '&lacute;': 'ĺ', '&lcaron;': 'ľ', '&lcedil;': 'ļ', '&lmidot;': 'ŀ',
  '&lstrok;': 'ł', '&nacute;': 'ń', '&napos;': 'ŉ', '&ncaron;': 'ň', '&ncedil;': 'ņ', '&odblac;': 'ő', '&omacr;': 'ō', '&racute;': 'ŕ',
  '&rcaron;': 'ř', '&rcedil;': 'ŗ', '&sacute;': 'ś', '&scedil;': 'ş', '&scirc;': 'ŝ', '&tcaron;': 'ť', '&tcedil;': 'ţ', '&tstrok;': 'ŧ',
  '&ubreve;': 'ŭ', '&udblac;': 'ű', '&umacr;': 'ū', '&uogon;': 'ų', '&uring;': 'ů', '&utilde;': 'ũ', '&wcirc;': 'ŵ', '&ycirc;': 'ŷ', '&zacute;': 'ź',
  '&zcaron;': 'ž', '&zdot;': 'ż'
       }[e], e) END;

-- Macro arguments are referenced ONCE (wrapped in `list_transform([s], lambda x: ...)[1]`): a macro that mentions its argument several times makes
-- nested calls explode exponentially at bind time (3 nested passes x 3 references = 27 copies of the 250-entry entity map).
-- one decode pass: all entities except &amp; first, then &amp; last
CREATE OR REPLACE TEMP MACRO hm_dec1(s) AS list_transform([s], lambda x:
  CASE WHEN x IS NULL OR NOT contains(x, '&') THEN x ELSE
    replace(
      list_reduce(list_filter(regexp_extract_all(x, '&(?:#[0-9]{1,7}|#[xX][0-9a-fA-F]{1,6}|[a-zA-Z][a-zA-Z0-9]{1,9});'), lambda e: e <> '&amp;'),
                  lambda acc, e: replace(acc, e, hm_ent(e)), x),
      '&amp;', '&')
  END)[1];
CREATE OR REPLACE TEMP MACRO hm_dec3(s) AS hm_dec1(hm_dec1(hm_dec1(s)));
-- entity decode only (author names): 3 passes, whitespace collapsed, empty -> NULL
CREATE OR REPLACE TEMP MACRO hm_dec(s) AS nullif(trim(regexp_replace(hm_dec3(s), '\s+', ' ', 'g')), '');

CREATE OR REPLACE TEMP MACRO hm_strip(s) AS list_transform([s], lambda x:
  CASE WHEN x IS NULL OR NOT contains(x, '<') THEN x ELSE
    regexp_replace(
      regexp_replace(x,
        '(?i)</?(?:sub|sup|i|b|em|strong|u|small|span|a|italic|bold|sc|scp|inf|tt|ital|roman|underline|overline|font|math|mi|mo|mn|mrow|msup|msub|msubsup|mover|munder|munderover|mtext|mfrac|msqrt|mroot|mstyle|mspace|mfenced|mpadded|mphantom|menclose|mmultiscripts|mprescripts|none|semantics|annotation|inline-formula|disp-formula|formula|tex-math|tex|[a-z]{1,5}:m[a-z]+|mml:[a-z0-9]+|jats:[a-z0-9-]+)(?:\s+[a-zA-Z:_-]+\s*=[^<>]*)?\s*/?>', '', 'g'),
      '(?i)</?(?:p|br|div|li|ul|ol|h[1-6]|tr|td|th|table|tbody|thead|blockquote|pre|hr|section|abstract|body|title)(?:\s+[a-zA-Z:_-]+\s*=[^<>]*)?\s*/?>', ' ', 'g')
  END)[1];

-- full text cleaning: entities (3 passes) -> whitelisted tags -> whitespace, empty -> NULL
CREATE OR REPLACE TEMP MACRO hm_clean(s) AS nullif(trim(regexp_replace(hm_strip(hm_dec3(s)), '\s+', ' ', 'g')), '');

-- list of texts: clean every element, drop the ones that become empty
CREATE OR REPLACE TEMP MACRO hm_clean_list(l) AS list_filter(list_transform(l, lambda x: hm_clean(x)), lambda x: x IS NOT NULL);

-- D19: name_key = normalised legalName + '|' + countryCode; used by the api to drop self-pairs / group duplicate institutions.
CREATE OR REPLACE TEMP MACRO hm_name_key(name, cc) AS
  trim(regexp_replace(lower(strip_accents(coalesce(hm_clean(name), ''))), '[^\p{L}\p{N}]+', ' ', 'g'))
  || '|' || lower(coalesce(cc, ''));

-- D28: year only inside 1950..2040 (the data has 1900..2125 junk); startDate/endDate stay raw for the overview.
CREATE OR REPLACE TEMP MACRO hm_year(d) AS CASE WHEN year(d) BETWEEN 1950 AND 2040 THEN year(d)::INTEGER END;

-- D23: works language. 3-letter ISO 639-2 codes, 30 "pairs" like fra/fre, esl/spa. Take the first part, map the ISO B-codes and
-- the non-ISO 'esl' to the T-code, 'und' -> NULL (36% of works: unknown, not a language).
CREATE OR REPLACE TEMP MACRO hm_lang(code) AS
  CASE WHEN code IS NULL OR lower(split_part(code, '/', 1)) IN ('und', '') THEN NULL
       ELSE coalesce(MAP {'alb': 'sqi', 'arm': 'hye', 'baq': 'eus', 'bur': 'mya', 'chi': 'zho', 'cze': 'ces', 'dut': 'nld', 'fre': 'fra',
                          'geo': 'kat', 'ger': 'deu', 'gre': 'ell', 'ice': 'isl', 'mac': 'mkd', 'mao': 'mri', 'may': 'msa', 'per': 'fas',
                          'rum': 'ron', 'slo': 'slk', 'tib': 'bod', 'wel': 'cym', 'esl': 'spa'}[lower(split_part(code, '/', 1))],
                     lower(split_part(code, '/', 1))) END;

-- ---- works: url extraction rules (D5/D25, verified on the cluster with 0 mismatches, results section G6). `&amp;` unescaped.
CREATE OR REPLACE TEMP MACRO wk_is_pdf_strict(u) AS regexp_matches(lower(split_part(u, '?', 1)), '\.pdf$');
CREATE OR REPLACE TEMP MACRO wk_is_pdf_loose(u) AS (regexp_matches(lower(split_part(u, '?', 1)), '\.pdf$') OR contains(lower(u), '/pdf'));
CREATE OR REPLACE TEMP MACRO wk_all_urls(ins) AS flatten(list_transform(ins, lambda i: coalesce(i.urls, [])));
CREATE OR REPLACE TEMP MACRO wk_open_urls(ins) AS
  flatten(list_transform(list_filter(ins, lambda i: i.accessRight.label = 'OPEN'), lambda i: coalesce(i.urls, [])));
CREATE OR REPLACE TEMP MACRO wk_doi_work(pids) AS
  regexp_replace(list_filter(pids, lambda p: lower(p.scheme) = 'doi')[1].value, '^https?://(dx\.)?doi\.org/', '');
CREATE OR REPLACE TEMP MACRO wk_doi_any(pids, ins) AS coalesce(wk_doi_work(pids),
  regexp_replace(list_filter(flatten(list_transform(ins, lambda i: coalesce(i.pids, []))), lambda p: lower(p.scheme) = 'doi')[1].value,
                 '^https?://(dx\.)?doi\.org/', ''));
-- pdf_url = first url of an OPEN instance ending in .pdf, else first OPEN url containing /pdf, else first .pdf of any instance, else first /pdf url
CREATE OR REPLACE TEMP MACRO wk_pdf_url(ins) AS replace(coalesce(
  list_filter(wk_open_urls(ins), lambda u: wk_is_pdf_strict(u))[1],
  list_filter(wk_open_urls(ins), lambda u: wk_is_pdf_loose(u))[1],
  list_filter(wk_all_urls(ins),  lambda u: wk_is_pdf_strict(u))[1],
  list_filter(wk_all_urls(ins),  lambda u: wk_is_pdf_loose(u))[1]), '&amp;', '&');
-- landing_url = https://doi.org/<doi>, else first OPEN url, else first url
CREATE OR REPLACE TEMP MACRO wk_landing_url(pids, ins) AS
  replace(coalesce('https://doi.org/' || wk_doi_any(pids, ins), wk_open_urls(ins)[1], wk_all_urls(ins)[1]), '&amp;', '&');
