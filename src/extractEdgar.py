from typing import Dict, List
import requests
import time
from dataclasses import dataclass


@dataclass
class FinancialFact:
    concept: str
    value: float
    start_date: str | None
    end_date: str
    filing_date: str
    frame: str | None
    unit: str
    form: str
    taxonomy: str
    label: str | None


class EDGARClient:
    XBRL_URL = 'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
    SEC_HEADERS =  {
            # Chrome 83 Windows
            "Upgrade-Insecure-Requests": "1",
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:134.0) Gecko/20100101 Firefox/134.0",
            "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,image/webp,image/apng,*/*;q=0.8,application/signed-exchange;v=b3;q=0.9",
            "Sec-Fetch-Site": "same-origin",
            "Sec-Fetch-Mode": "navigate",
            "Sec-Fetch-User": "?1",
            "Sec-Fetch-Dest": "document",
            "Referer": "https://www.google.com/",
            "Accept-Encoding": "gzip, deflate, br",
            "Accept-Language": "en-US,en;q=0.9"
        }

    def __init__(self, cik: str):
        self.cik = self._format_cik(cik)

    def _format_cik(self, cik: str) -> str:
        """Format CIK to 10 digits with leading zeros"""
        return str(cik).zfill(10)

    def _make_request(self, url: str) -> requests.Response:
        """Make HTTP request with SEC rate limiting"""
        time.sleep(0.3)
        response = requests.get(url, headers=self.SEC_HEADERS)
        response.raise_for_status()
        return response

    def get_all_facts(self) -> Dict[str, List[FinancialFact]]:
        """Fetch all XBRL facts without filtering by filing type"""
        url = self.XBRL_URL.format(cik=self.cik)
        try:
            response = self._make_request(url)
            data = response.json()
            return self._parse_facts(data)
        except Exception as e:
            raise Exception(f"Failed to fetch XBRL facts: {str(e)}")
    
    def _parse_facts(self, data: dict) -> Dict[str, List[FinancialFact]]:
        """Parse raw JSON data into FinancialFact objects"""
        facts_by_concept = {}
        
        for taxonomy, concepts in data['facts'].items():
            for concept, details in concepts.items():
                for unit_type, facts in details['units'].items():
                    facts_list = []
                    for fact in facts:
                        try:
                            financial_fact = FinancialFact(
                                concept=concept,
                                value=float(fact['val']),
                                start_date=fact.get('start'),
                                end_date=fact['end'],
                                filing_date=fact['filed'],
                                frame=fact.get('frame'),
                                unit=unit_type,
                                form=fact.get('form', 'UNKNOWN'),
                                taxonomy=taxonomy,
                                label=details.get('label')
                            )
                            facts_list.append(financial_fact)
                        except (ValueError, KeyError) as e:
                            print(f"Skipping invalid fact for {concept}: {str(e)}")
                    if facts_list:
                        key = f"{taxonomy}:{concept}:{unit_type}"
                        facts_by_concept[key] = facts_list
        return facts_by_concept    
