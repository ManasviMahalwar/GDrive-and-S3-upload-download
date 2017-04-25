from __future__ import print_function
from googleapiclient.http import MediaIoBaseDownload
import httplib2
import os
import sys
import datetime
import time

from apiclient import discovery
import io
import oauth2client
from oauth2client import client
from oauth2client import tools
from apiclient import discovery
from oauth2client.file import Storage

from logbook import Logger, FileHandler, StreamHandler
from progress_bar import InitBar

import boto3


log = Logger('google-drive-to-s3')
try:
    #Accept param from user through commanc line
    import argparse
    Params = argparse.ArgumentParser(parents=[tools.argparser])
    Params.add_argument('--folder_id', '-f', type=str, required=True,
                       help="Google Drive Folder ID (it's the end of the folder URI!) (required)")
    Params.add_argument('--bucket', '-b', type=str, required=True,
                       help="Name of S3 bucket to use (required)")
    Params.add_argument('--keyprefix', '-k', type=str, default=None,
                       help="Key prefix to use as the path to a folder in S3 where to upload file (defaults to upload files in bucket directly without any folder)")
    Params.add_argument('--loglevel', type=str, help='Choose a log level', default='INFO')
    Params.add_argument('--pagesize', '-p', type=int, default=100,
                       help="Number of files in each page (defaults to 100)")
    Params.add_argument('--startpage', '-s', type=int, default=1,
                       help="start from page N of the file listing (defaults to 1)")
    Params.add_argument('--endpage', '-e', type=int, default=None,
                       help="stop paging at page N of the file listing (defaults to not stop before the end)")
    Params.add_argument('--matchfile', type=str, default=None,
                       help="Only process files if the filename is in this file (defaults to process all files)")

    args = Params.parse_args()
except ImportError:
    args = None

# If modifying these scopes, delete your previously saved credentials
# at ~/.credentials/drive-python-quickstart.json
SCOPES = 'https://www.googleapis.com/auth/drive'
CLIENT_SECRET_FILE = 'client_secret.json'
APPLICATION_NAME = 'Drive API MM'

#global var
readbyte=io.BytesIO()


def get_authorized_google_http():
    """Gets valid user credentials from storage.

    If nothing has been stored, or if the stored credentials are invalid,
    the OAuth2 flow is completed to obtain the new credentials.

    Returns:
        Credentials, the obtained credential.
    """
    try:
        home_dir = os.path.expanduser('~')
        credential_dir = os.path.join(home_dir, '.credentials')
        if not os.path.exists(credential_dir):
            os.makedirs(credential_dir)
        credential_path = os.path.join(credential_dir,
                                       'drive-python-quickstart.json')

        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(CLIENT_SECRET_FILE, SCOPES)
            flow.user_agent = APPLICATION_NAME
            if args:
                credentials = tools.run_flow(flow, store, args)
            else: # Needed only for compatibility with Python 2.6
                credentials = tools.run(flow, store)
            log.info('Storing credentials to ' + credential_path)
            """get credentials and build authorized google drive service"""

        http = credentials.authorize(httplib2.Http())
        return http
    except (Exception) as e:
        raise (Exception)
        log.error('Error in creating http connection to Gdrive' + str(e))

def print_allFiles(result):
    """printing files in retrieved list to test"""
    items = result.get('files', [])
    if not items:
        print('No files found.')
    else:
        print('Files:')
        for item in items:
            print('{0} ({1})'.format(item['name'], item['id']))

def print_arguments(args):
    """printing argument we got to test.. only imp one for debugging, value are registered in log"""
    print("You have passed following folder ID :", args.folder_id)

def setup_logging():
    try:

        print("setting up log file")
        #get current folder
        current_dir_path = os.path.dirname(os.path.realpath(__file__))
        #join log folder to current path, if you \log then it will be created one level up
        log_path=os.path.join(current_dir_path,"log")
        log.info('log path:' + log_path)
        #check if log folder exist
        if not os.path.exists(log_path):
            try:
                print("creating log folder")
                os.makedirs(log_path)
            except OSError as exc:  # Guard against race condition
                if exc.errno != errno.EEXIST:
                    raise
        else:
            log.info("Log folder exist")

        log.info("create log file inside log folder")
        log_filename = os.path.join(
            log_path,
            'google-drive-to-s3-{}.log'.format(os.path.basename(time.strftime('%Y%m%d-%H%M%S')))
        )
        print(log_filename)
        """#test to create log file on disk
        if not os.path.exists(log_filename):
            print("create log file")
            try:
                # double check against OS race
                filehandle = open(log_filename, 'r')
            except IOError:
                # if file does not exist, create it
                filehandle = open(log_filename, "w")
        """
        # register some logging handlers
        log_handler = FileHandler(
            log_filename,
            mode='w',
            level=args.loglevel,
            bubble=True
        )
        return log_handler
    except (Exception) as e:
        log.error('Error in creating log handler' + str(e))

def ensure_trailing_slash(val):
    if val[-1] != '/' :
        return "{}/".format(val)
    return val


def create_aws_client():
    try:
        """makin connection to S3 bucket, we can authroize the credential 
           or we can configure in ~/.aws/config (in your home dir)file. """
        log.info("Attempting S3 connection")
        """created sepreate user for upload with s3 full access policy and only cli login"""
        session = boto3.session.Session(region_name='us-east-1', aws_access_key_id='Your id',
                                        aws_secret_access_key='your key')
        # Let's use Amazon S3
        """I got error for auth mechanism - AWS-HMAC-SHA256, to solve use custom session and config as below"""
        s3client = session.client('s3', config=boto3.session.Config(signature_version='s3v4'))
        log.info('S3 client created')
        return s3client

    except (Exception) as e:
        raise (Exception)
        log.error('Error in creating S3 client' + str(e))

def matchFileName(filename, match_files):
    if not match_files:  # We have not supplied any file names to match against, so process everything.
        return True
    if filename in match_files:
        return True
    return False

def download_from_Gdrive(gservice,this_file):
    try:
        #download current file into memory
        download_request = gservice.files().get_media(fileId=this_file['id'])
        readbyte.flush()  # Using an in memory stream location
        downloader = MediaIoBaseDownload(readbyte, download_request)
        done = False
        pbar = InitBar('Downloading: '+ this_file['name'])
        while done is False:
            status, done = downloader.next_chunk()
            pbar(int(status.progress() * 100))
        del pbar
        # tempbyte.close()
        return readbyte
    except (Exception) as e:
        log.error('Error in downloading file from gdrive' +this_file['name'])
        log.error('\n'+str(e))

def upload_to_s3(bucket,keyprifix,tempbyte,s3client,this_file):
    try:
        # Upload a new local file to test S3
        # data = open('Deser1t.jpg', 'rb')
        # s3client.put_object(Bucket='bucketMM',Key='Deser22t.jpg', Body=data, ACL='private')

        # upload Gdrive file to s3from memory
        if args.keyprefix == None:
            log.info('Key-prefix not provided, attempting to upload in bcuket - ' + args.bucket)
            s3client.put_object(Bucket=args.bucket, Key=this_file['name'], Body=tempbyte.getvalue(), ACL='private')
            log.info('Upload complete for file:' + this_file['name'])
            return True


        else:
            # make sure our S3 Key prefix has a trailing slash
            key_prefix = ensure_trailing_slash(args.keyprefix)
            log.info('Key-prefix provided, attempting to upload in folder-' + args.bucket + '/' + key_prefix)
            s3client.put_object(Bucket=args.bucket, Key="{}{}".format(key_prefix, this_file['name']),
                                Body=tempbyte.getvalue(), ACL='private')
            log.info('Upload complete for file:' + this_file['name'])
            return True

    except (Exception) as e:
        log.error('Error in uploading file from S3' + this_file['name'])
        log.error('\n' + str(e))
        return False




def main():
    """printing argument we got to test"""
    #print_arguments(args=args)
    file_count = 0
    page_count = 0
    upload_count = 0

    log_handler= setup_logging()

    stdout_handler = StreamHandler(sys.stdout, level=args.loglevel, bubble=True)

    with stdout_handler.applicationbound():
        with log_handler.applicationbound():
            log.info("MM Python log file test")
            log.info("Arguments: {}".format(args))
            start = time.time()
            log.info("starting at {}".format(time.strftime('%I:%M%p %Z on %b %d, %Y')),start)
            """get credentials and build authorized google drive service"""
            http = get_authorized_google_http()
            gdrive_service = discovery.build('drive', 'v3', http=http)
            #create s3 client for upload
            s3client = create_aws_client()

            # load up a match file if we have one.


            """getting files in the selected folder passed in the argument"""
            query = gdrive_service.files().list(
              pageSize=args.pagesize,q="'{}' in parents".format(args.folder_id),fields="nextPageToken, files(id, name)")


            #get all the files and read byte
            while query is not None:
                try:
                    result_gfiles = query.execute(http=http)
                    page_count += 1
                    print("\nwhile loop query, page number:" + str(page_count))

                    #printing files in retrieived list to test
                    #print_allFiles(result=result_gfiles)
                    # determine the page at which to start processing as per user input
                    if page_count >= args.startpage:
                        log.info(u"######## Page {} ########".format(page_count))
                        for this_file in result_gfiles.get('files', []):
                            print('\n Number of file processed: ' + str(file_count))
                            log.info('Current file ID and name under process: ' +this_file['id'] + ',' + this_file['name'])
                            file_count+=1
                            if args.matchfile != None:
                                log.info('Match file parameter passed by user:' + args.matchfile)
                                if matchFileName(this_file['name'], args.matchfile):
                                    log.info('match found:' + this_file['name'])
                                    # download from gdrive
                                    tempbyte = download_from_Gdrive(gdrive_service, this_file)
                                    # upload to s3
                                    val=upload_to_s3(args.bucket, args.keyprefix, tempbyte, s3client, this_file)
                                    if val==True:
                                        log.info('Upload complete for file:'+ this_file['name'])
                                        upload_count += 1

                                    break

                                else:
                                    log.info('File not macthed, file from gdrive: ' + this_file['name'] + ',  matching file name by user( '+ args.matchfile + ') not found on page-' + str(page_count))

                            else:
                                log.info('Match file parameter not passed by user,uploading all file on this page:'+ str(page_count) )
                                #download from gdrive
                                tempbyte = download_from_Gdrive(gdrive_service,this_file)
                                #upload to s3
                                val= upload_to_s3(args.bucket,args.keyprefix,tempbyte,s3client,this_file)
                                if val == True:
                                    log.info('Upload complete for file:' + this_file['name'])
                                    upload_count += 1


                    # stop if we have come to the last user specified page
                    if args.endpage and page_counter == args.endpage:
                         log.info(u"User defined engapage reached, finished paging at page {}".format(page_counter))
                         readbyte.close()
                         log.info("Running time: {}".format(str(datetime.timedelta(seconds=(round(time.time() - start, 3))))))
                         log.info('Total files processed:' + str(file_count))
                         log.info('Total files uploaded: '+ str(upload_count))
                         break
                    page_token = result_gfiles.get('nextPageToken')
                    if not page_token:
                        print('\nLast page token')
                        log.info('Reached the end of pagination, All files traversed')
                        readbyte.close()
                         log.info("Running time: {}".format(str(datetime.timedelta(seconds=(round(time.time() - start, 3))))))
                         log.info('Total files processed:' + str(file_count))
                         log.info('Total files uploaded: '+ str(upload_count))
                        break
                except (Exception) as e:
                    log.error ('An error occurred: '+ str(e) )
                    readbyte.close()
                    log.error(e)
                    break

    print('\n program ended')
   
    readbyte.close()










#Execution start from here.. like Java main
if __name__ == '__main__':
    main()
