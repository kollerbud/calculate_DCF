from typing import Dict, List, Optional
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


@dataclass
class FilingInfo:
    index: int
    form: str
    accessionNumber: str
    accessionNumberNoDash: str
    primaryDocument: str
    filingDate: str


class EDGARClient:
    XBRL_URL = 'https://data.sec.gov/api/xbrl/companyfacts/CIK{cik}.json'
    SUBMISSIONS_URL = 'https://data.sec.gov/submissions/CIK{cik}.json'
    FILING_DOC_URL = 'https://www.sec.gov/Archives/edgar/data/{cik}/{accession_no_dash}/{primary_doc}'

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

    SEC_REQUEST_DELAY = 0.25

    def __init__(self, cik: str):
        self.cik = self._format_cik(cik=cik)

    def _format_cik(self, cik: str) -> str:
        """Format CIK to 10 digits with leading zeros"""
        return str(cik).zfill(10)

    def _make_request(self, url: str) -> requests.Response:
        """Make HTTP request with SEC rate limiting"""
        time.sleep(self.SEC_REQUEST_DELAY)
        response = requests.get(url, headers=self.SEC_HEADERS)
        response.raise_for_status()
        return response

    def get_company_facts(self) -> Dict[str, List[FinancialFact]]:
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
        if 'facts' not in data:
            print("Warning: 'facts' key not found in company facts data.")
            return {}

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

    def get_submissions(self) -> Dict[str, List]:

        url = self.SUBMISSIONS_URL.format(cik=self.cik)
        try:
            response = self._make_request(url=url)
            return response.json()
        except requests.exceptions.RequestException as req_err:
            print(f"HTTP Error fetching submissions: {req_err}")
            raise Exception(f"Failed to fetch submissions: {req_err}") from req_err
        except Exception as e:
            print(f"Error processing submissions data: {e}")
            raise Exception(f"Failed to process submissions data: {e}") from e

    def get_recent_filing_info(
            self,
            target_forms: Optional[List[str]] = None
        ) -> List[FilingInfo]:

        if target_forms is None:
            target_forms_set = {'10-K', '10-Q'}
        else:
            target_forms_set = set(target_forms)

        try:
            submission_data = self.get_submissions()
            return self._parse_recent_filings(submission_data, target_forms_set)
        except Exception as e:
            print(f"Could not get or parse filing info: {e}")
            return []


    def _parse_recent_filings(
            self,
            submission_data: dict,
            target_forms_set: set
        ) -> List[FilingInfo]:

        filings_data = submission_data.get('filings', {}).get('recent', {})
        if not filings_data:
            print("Warning: 'filings.recent' structure not found in submission data.")
            return []

        forms = filings_data.get('form', [])
        accession_numbers = filings_data.get('accessionNumber', [])
        primary_documents = filings_data.get('primaryDocument', [])
        filing_dates = filings_data.get('filingDate', [])

        list_len = len(forms)
        if not (list_len == len(accession_numbers) == len(primary_documents) == len(filing_dates)):
            print("Warning: Mismatch in lengths of recent filing data lists. Results may be inaccurate.")
            list_len = min(len(forms), len(accession_numbers), len(primary_documents), len(filing_dates))

        found_filings: List[FilingInfo] = []

        for index in range(list_len):
            form_type = forms[index]
            if form_type in target_forms_set:
                original_acc_num = accession_numbers[index]
                acc_num_no_dash = original_acc_num.replace('-', '')

                filing_info: FilingInfo = {
                    'index': index,
                    'form': form_type,
                    'accessionNumber': original_acc_num,
                    'accessionNumberNoDash': acc_num_no_dash,
                    'primaryDocument': primary_documents[index],
                    'filingDate': filing_dates[index]
                }

                found_filings.append(filing_info)

        if not found_filings:
                print(f"No filings matching {target_forms_set} found in recent filings.")
        return found_filings

    def get_filing_document(
        self,
        accession_no_dash: str,
        primary_doc: str
        ) -> str:

        url = self.FILING_DOC_URL.format(
            cik=self.cik,
            accession_no_dash=accession_no_dash,
            primary_doc=primary_doc
        )
        print(f"Fetching filing document from: {url}")
        try:
            response = self._make_request(url)
            return response.text
        except requests.exceptions.RequestException as req_err:
            print(f"HTTP Error fetching filing document {primary_doc}: {req_err}")
            raise Exception(f"Failed to fetch filing document: {req_err}") from req_err
        except Exception as e:
            print(f"Error processing filing document {primary_doc}: {e}")
            raise Exception(f"Failed to process filing document: {e}") from e


if __name__ == '__main__':
    # https://www.sec.gov/Archives/edgar/data/1045810/000104581024000316/nvda-20241027.htm
    # 000104581024000316 <- accessionNumber without dash
    # nvda-20241027.htm -< primaryDocument
    print(EDGARClient(cik='1045810').get_filing_document(accession_no_dash='000104581024000316', primary_doc='nvda-20241027.htm'))