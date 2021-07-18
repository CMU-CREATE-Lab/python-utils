import dateutil, io, json, re, requests
import pandas as pd

class GSheet:
    def __init__(self, file_id, gid=None):
        self.file_id = file_id
        self.gid = gid

    @classmethod
    def from_file_id_gid(cls, file_id_gid):
        return cls(*file_id_gid.split('.', 2))

    @classmethod
    def from_url(cls, url):
        regex = r'https?://docs.google.com/spreadsheets/d/(\w+)/(.*gid=(\d+))?'
        match = re.match(regex, url)
        if not match:
            raise Exception(f'url {url} does not match regex {regex}, aborting')
        return cls(file_id=match.group(1), gid=match.group(3))

    @property
    def file_id_gid(self):
        ret = self.file_id
        if self.gid:
            ret += '.' + self.gid
        return ret

    @property
    def url(self):
        ret = f'https://docs.google.com/spreadsheets/d/{self.file_id}/edit'
        if self.gid:
            ret += f'#gid={self.gid}'
        return ret

    def get_modtime(self):
        api_key = json.load(open('keys/google_drive_api_key.json'))['api_key']
        api_url = f'https://www.googleapis.com/drive/v3/files/{self.file_id}?fields=modifiedTime&key={api_key}'
        print(api_url)
        response = requests.get(api_url).json()
        return dateutil.parser.isoparse(response['modifiedTime'])

    def get_last_revision_modtime(self):
        service = get_gdrive_service()
        f = service.revisions().list(fileId = '1heLmeuPp7j4itr0cK8H4chugOpp7cU8p_VEB9CFfPlY')
        revs = f.execute()
        return dateutil.parser.isoparse(revs['revisions'][-1]['modifiedTime'])

    def get_csv_export_url(self):
        ret = f'https://docs.google.com/spreadsheets/d/{self.file_id}/export?format=csv'
        if self.gid:
            ret += f'&gid={self.gid}'
        return ret
    
    debug = None

    def read_csv(self, record_modtimes=None):
        if record_modtimes is not None:
            record_modtimes[self.file_id] = self.get_modtime()
        url = self.get_csv_export_url()
        csv = requests.get(url).text
        try:
            csv_df = pd.read_csv(io.StringIO(csv), keep_default_na=False, dtype={'Enabled':str,'Share link identifier':str})
        except Exception as e:
            print('Catching exception in GSheet.read_csv.  Setting GSheet.debug')
            GSheet.debug = dict(url=url, csv=csv)
            raise
        # Get rid of any carriage returns or tabs (which google would have already done if we'd requested tsv)
        csv_df = csv_df.replace({'\n|\r|\t':''},regex=True)
        print(f'Read {self.get_csv_export_url()} ({len(csv_df)} rows)')
        return csv_df

    def __repr__(self):
        return self.file_id_gid
