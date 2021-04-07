#%%
from googleapiclient.discovery import build
from googleapiclient.http import MediaIoBaseDownload, MediaIoBaseUpload
from google_auth_oauthlib.flow import InstalledAppFlow
from google.auth.transport.requests import Request
from googleapiclient.errors import HttpError

'reload_module' in vars() and reload_module('utils.utils')
from utils.utils import *
reload_module('google_creds')
import google_creds
reload_module('pdf_utils')
from pdf_utils import *

creds = google_creds.get_creds()
docs_service = build('docs', 'v1', credentials=creds)
drive_service = build('drive', 'v3', credentials=creds)

class GDriveItem:
    def __init__(self, url_or_id, name=None, mimeType=None):
        match = re.search(r'drive/folders/(.*)', url_or_id)
        if match:
            id = match[1]
            mimeType = 'application/vnd.google-apps.folder'
        else:
            id = url_or_id

        self.id = id
        self.name = name
        self.mimeType = mimeType
    
    def children(self, query=None):
        if self.mimeType:
            assert self.mimeType == 'application/vnd.google-apps.folder'
        q = f"'{self.id}' in parents"
        if query:
            q = f"{q} and ({query})"
        try:
            results = drive_service.files().list(q= q, spaces='drive').execute()
        except Exception as e:
            print(e)
            raise
        ret = [GDriveItem(f['id'], f['name'], f['mimeType']) for f in results['files']]
        print(f'{self} children({q}) returns {len(ret)} items')
        return sorted(ret, key=lambda f:f.name)
    
    def child_doc_files(self):
        return self.children("mimeType = 'application/vnd.google-apps.document'")

    def child(self, name):
        children = self.children(f"name = '{name}'") # TODO: quote {name}
        if len(children) == 1:
            return children[0]
        elif len(children) == 0:
            raise Exception(f'File "{name}" not found inside folder {self}')
        else:
            raise Exception(f'File "{name}" found {len(children)} times in folder {self}???')

    def export_to_pdf(self):
        try:
            data = drive_service.files().export(fileId=self.id, mimeType='application/pdf').execute()
        except Exception as e:
            print(e)
            raise
        print(f'Converted {self} to PDF ({len(data)} bytes)')
        return data

    def export_to_pdf_file(self, dest_filename):
        pdf_data = self.export_to_pdf()
        open(dest_filename, 'wb').write(pdf_data)
        print(f'Wrote {len(pdf_data)} bytes to {dest_filename}')

    def export_children_to_pdf_and_concat(self, dest_filename):
        pdf_merger = PdfFileMerger()
        for gdoc in self.child_doc_files():
            pdf = gdoc.export_to_pdf()
            pdf_merger.append(io.BytesIO(pdf))
        pdf_merger.write(dest_filename)
        print(f'Wrote {os.path.getsize(dest_filename)} bytes to {dest_filename}')

    def __str__(self):
        if self.name:
            return f"[{self.name} ({self.id})]"
        else:
            return f"[{self.id}]"

    def __repr__(self):
        return self.__str__()

    def content(self):
            request = drive_service.files().get_media(fileId=self.id)
            out = io.BytesIO()
            downloader = MediaIoBaseDownload(out, request)
            done = False
            while done is False:
                status, done = downloader.next_chunk()
            out.seek(0)
            return out.read()
    
    def upload(self, name, mimeType, content):
        file_metadata = dict(name=name, mimeType=mimeType, parents=[self.id])
        media = MediaIoBaseUpload(
            io.BytesIO(content), mimetype=mimeType, resumable=True)
        try:
            fields = drive_service.files().create(
                body=file_metadata, media_body=media, 
                fields='id,name,mimeType,webViewLink').execute()
        except Exception as e:
            print(e)
            raise
        return GDriveItem(fields['id'], name=fields['name'], mimeType=fields['mimeType'])

# parent_folder_gdi is a GDriveItem to be the parent of the child folder
def create_child_folder(name, parent_folder_gdi):
    file_metadata = dict(name=name, mimeType='application/vnd.google-apps.folder', parents=[parent_folder_gdi.id])
    fields = drive_service.files().create(body=file_metadata,fields='id,name,mimeType').execute()
    new_folder = GDriveItem(fields['id'], name=fields['name'], mimeType=fields['mimeType'])
    return new_folder
