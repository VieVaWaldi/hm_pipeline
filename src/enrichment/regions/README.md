# Regions

`country_regions.csv`: ISO alpha-2 (plus XK) to one of `Northern Europe`, `Central Europe`,
`Western Europe`, `Eastern Europe`, `Southern Europe` or `Outside Europe`. It covers every code
`common.countries` accepts (249 ISO + XK), so a normalised country always finds a row.

Basis: Wikipedia's [Regions of Europe](https://en.wikipedia.org/wiki/Regions_of_Europe), reduced to
five regions. Judgement calls (edit the CSV, no code change needed):

| code | region | why |
|---|---|---|
| GB, IE, IM, JE, GG | Northern | British Isles sit in Northern Europe in the UN geoscheme |
| CH, LI, AT | Central | Central Europe, not Western |
| SI | Central | Central Europe (UN geoscheme has it Southern) |
| RO, BG | Eastern | not Southern (Balkans) |
| CY | Southern | EU member, geographically Asia, UN says Western Asia |
| TR, AM, AZ, GE, KZ | Outside Europe | transcontinental / Asian |
| RU | Eastern | |
| GL | Outside Europe | geographically North America |
| XK | Southern | Western Balkans |
