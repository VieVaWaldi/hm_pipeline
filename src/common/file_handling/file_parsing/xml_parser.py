import os
import xml.etree.ElementTree as eT
from pathlib import Path
from typing import List, Dict, Iterator, Any

import xmltodict

from common.errors.error_handling import log_and_exit


def get_xml_as_dict_recursively(file_path: Path) -> Iterator[Dict[str, Any]]:
    """
    Searches all XML files in all subdirectories under the file_path and
    Returns a generator with each file as an entire dictionary.
    """
    yield from _process_xml_files(file_path, extract_xml_as_dict)


def get_all_elements_text_recursively(
    file_path: Path, element_name: str
) -> Iterator[List[Any]]:
    """
    Searches all XML files in all subdirectories under the file_path for element_name.
    Returns a generator of all texts for all matching elements.
    """
    yield from _process_xml_files(
        file_path, lambda fp: extract_element_texts(fp, element_name)
    )


def _process_xml_files(file_path: Path, extraction_func) -> Iterator[Any]:
    """
    Walks through all XML files in the given file_path and applies the extraction_func to each.
    """
    for root, _, files in os.walk(file_path):
        for file in files:
            if file.endswith(".xml"):
                full_file_path = os.path.join(root, file)
                yield extraction_func(full_file_path)


def extract_xml_as_dict(file_path: Path) -> Dict[str, Any]:
    """
    Extracts the entire XML file as a dictionary.
    """
    try:
        tree = eT.parse(file_path)
        root = tree.getroot()
        return element_to_dict(root)
    except eT.ParseError:
        log_and_exit(f"Error parsing XML file: {file_path}")


def extract_element_as_dict(file_path: Path, element_name: str) -> List[Dict[str, str]]:
    """
    Extracts the specified element from an XML file as a dictionary,
    for each matching element.
    """
    try:
        tree = eT.parse(file_path)
        root = tree.getroot()
        return [
            element_to_dict(elem) for elem in root.iter() if elem.tag == element_name
        ]
    except eT.ParseError:
        log_and_exit(f"Error parsing XML file: {file_path}")


def element_to_dict(element: eT.Element) -> Dict[str, str]:
    """
    Converts an XML element to a dictionary using xmltodict.
    If a leaf element has attributes, its value is a dict,
    where its text will be the value of the key #text
    and its attributes will be keys starting with @.
    """
    xml_string = eT.tostring(element, encoding="unicode")
    return xmltodict.parse(xml_string)


def extract_element_texts(file_path: Path, element_name: str) -> List[str]:
    """
    Extracts the text of the specified element from an XML file.
    """
    try:
        tree = eT.parse(file_path)
        root = tree.getroot()
        return [
            elem.text
            for elem in root.iter()
            if elem.tag == element_name and elem.text is not None
        ]
    except eT.ParseError:
        log_and_exit(f"Error parsing XML file: {file_path}")
