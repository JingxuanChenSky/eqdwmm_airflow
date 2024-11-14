import logging as log
import os
import requests
import sys

log.basicConfig(level=log.INFO, stream=sys.stdout, format='%(asctime)s - %(levelname)s - %(message)s')

def HKEx_ListOfSecuritiesDownload(**kwargs):
    save_path = kwargs['Path']
    as_of_df = kwargs['ExecDate']

    listOfSecuritiesUrl = 'https://www.hkex.com.hk/eng/services/trading/securities/securitieslists/ListOfSecurities.xlsx'
    req = requests.get(listOfSecuritiesUrl, verify=False)

    tDay = as_of_df.strftime('%Y%m%d')
    reqFile = open(os.path.join(save_path, 'listOfSecurities_{}.xlsx'.format(tDay)), 'wb')
    reqFile.write(req.content)
    reqFile.close()