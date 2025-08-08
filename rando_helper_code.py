import os
import time
import requests
from pyairtable import Api
from documentcloud import DocumentCloud

api = Api(os.environ['AIRTABLE_PAT'])
airtab = api.table(os.environ['suffrage_db'], 'suffrage restoration bills')

dc = DocumentCloud(os.environ['MUCKROCK_USERNAME'], os.environ['MUCKROCK_PW'])

def airtab_to_dc_and_back():
    records = airtab.all(view='needs dc', fields='msleg_pdf_url', max_records=100)
    print(f"{len(records)} records have PDFs that need to be uploaded to DC...")
    for record in records:
        this_dict = {}
        pdf_url = record['fields']['msleg_pdf_url']
        try:
            obj = dc.documents.upload(pdf_url, access='public')
        except requests.exceptions.ReadTimeout:
            time.sleep(5)
            # print('sleeping bc readtimeout')
            continue
        obj = dc.documents.get(obj.id)
        while obj.status != 'success':
            time.sleep(5)
            # print('sleeping bc status is not "success"')
            obj = dc.documents.get(obj.id)
        this_dict["dc_id"] = str(obj.id)
        print(f"successfully uploaded {obj.title}. . .")
        this_dict["dc_pdf_url"] = obj.pdf_url
        this_dict["dc_canonical_url"] = obj.canonical_url
        this_dict["dc_full_text_url"] = obj.full_text_url
        this_dict["bill_text"] = obj.full_text
        airtab.update(record["id"], this_dict, typecast=True)

def main():
    print('the rando helper is helping!')
    airtab_to_dc_and_back()


if __name__ == "__main__":
    main()
