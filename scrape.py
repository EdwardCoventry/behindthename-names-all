import argparse
import csv
from enum import Enum
from itertools import count
import logging
import os
import sys
from typing import Optional, List, Iterable
import regex as re

import attr
from bs4 import BeautifulSoup, SoupStrainer
import requests


class NameKind(Enum):
    FIRST_NAME = "first_name"
    SURNAME = "surname"


BASE_URLS = {
    NameKind.SURNAME: "https://surnames.behindthename.com/",
    NameKind.FIRST_NAME: "https://www.behindthename.com/",
}

root_logger = logging.getLogger()
ch = logging.StreamHandler(sys.stderr)
root_logger.handlers = [ch]
_log = logging.getLogger(__name__)


def get_and_assert_ok(url: str):
    r = requests.get(url)
    assert r.status_code == 200
    return r


@attr.s(auto_attribs=True, frozen=True, slots=True)
class Name:
    description: str
    text: str
    usage: str

    @staticmethod
    def from_listing(soup) -> "Name":
        usage = (soup.contents[1].text,)
        text = (soup.contents[0].text,)

        soup.contents = soup.contents[2:]
        description = soup.text
        return Name(description=description, usage=usage[0], text=text[0])


@attr.s(auto_attribs=True, frozen=True, slots=True)
class BehindTheNamesSite:
    base_url: str

    def _names_list_url(self, i: int) -> Name:
        url = self.base_url + "names"
        if i > 0:
            url = url + "/{}".format(i + 1)
        return url

    def scrape_all_names(self):
        _log.info("Connecting to {}".format(self.base_url))
        for page_index in count():
            _log.info("Scraping page {}".format(page_index))
            results_from_page = 0
            response = get_and_assert_ok(self._names_list_url(page_index))
            for name in scrape_names_results(response.text):
                results_from_page = results_from_page + 1
                yield name

            _log.info("Scraped {} names".format(results_from_page))
            if results_from_page == 0:
                break


def scrape_names_results(text: str) -> Iterable[Name]:
    soup = BeautifulSoup(
        text,
        "html.parser",
        parse_only=SoupStrainer("div", class_="browsename"),
    )
    return map(Name.from_listing, soup)

def clean_name(name):
    name = re.sub("\d+", "", name)
    name = re.sub("[ ]+", " ", name)
    name = name.strip()
    name = name.rstrip('.,)!?')
    return name

def yield_scrape_pairs(base_url: str):
    names = BehindTheNamesSite(base_url).scrape_all_names()

    for name in names:

        if unvariant_match := re.search('variant of (.*)', name.description, flags=re.IGNORECASE):

            # variant_name = clean_name(name.text)
            # original_name = clean_name(unvariant_match.group(1))

            variant_name = clean_name(name.text)
            original_name = clean_name(unvariant_match.group(1).split()[0])

            if not original_name[0].isupper():
                continue

            if ',' in variant_name+original_name:
                continue

            # add eg Edvaard -> Edward or Edvard
            if (or_match := re.match('([^\s]+) or ([^\s]+)', unvariant_match.group(1).strip())) \
                    and all(or_match.group(x)[0].isupper() for x in (1, 2)):
                for x in (1, 2):
                    yield variant_name, clean_name(or_match.group(x))
            else:
                yield variant_name, original_name

def write_scrape(base_url: str, f):

    for variant, original in yield_scrape_pairs(base_url):
        if variant != original:
            f.write(f"{variant},{original}\n")


def main_parser():
    parser = argparse.ArgumentParser(
        "Scrape behindthename.com for full lists of names"
    )
    parser.add_argument(
        "kind",
        choices=list(nk.value for nk in NameKind),
        nargs="?",
        default=NameKind.FIRST_NAME.value,
        help="Kind of name to scrape.",
    )
    return parser


def main(f):
    parser = main_parser()
    args = parser.parse_args()
    root_logger.setLevel(logging.INFO)
    _log.setLevel(logging.INFO)
    return write_scrape(BASE_URLS[NameKind(args.kind)], f)


if __name__ == "__main__":

    with open('variantstooriginals.csv.csv', 'w', encoding='utf-8') as f:
        main(f)
