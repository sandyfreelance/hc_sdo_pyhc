import scregistry_prerelease as scregistry
import boto3
import dask
import io
import logging
import time
import re
import pickle
import astropy.io.fits
try:
    from dask.distributed import Client
    daskMe = True
except:
    print("You do not have Dask, cannot do parallelized work")
    daskMe = False


def DO_SCIENCE(mydata):
    # you can put better science here
    iirad = mydata.mean()
    return iirad


# these are variable helpful handler functions
def s3url_to_bucketkey(s3url: str): # -> Tuple[str, str]:
    """
    Extracts the S3 bucket name and file key from an S3 URL.
    e.g. s3://mybucket/mykeypart1/mykeypart2/fname.fits -> mybucket, mykeypart1/mykeypart2/fname.fits
    """
    name2 = re.sub(r"s3://","",s3url)
    s = name2.split("/",1)
    return s[0], s[1]

def process_fits_s3(s3key:str): # -> Tuple[str, float]:
    """ For a single FITS file, read it from S3, grab the header and
        data, then do the DO_SCIENCE() call of choice
    """
    sess = boto3.session.Session() # do this each open to avoid thread problem 'credential_provider'
    s3c = sess.client("s3")
    mybucket,mykey = s3url_to_bucketkey(s3key)
    try:
        fobj = s3c.get_object(Bucket=mybucket,Key=mykey)
        rawdata = fobj['Body'].read()
        bdata = io.BytesIO(rawdata)
        hdul = astropy.io.fits.open(bdata,memmap=False)        
        date = hdul[1].header['T_OBS']
        irrad = DO_SCIENCE(hdul[1].data)
    except:
        print("Error fetching ",s3key)
        date, irrad = None, None
        
    return date, irrad


    
# Actual work now
    
fr=scregistry.FileRegistry("s3://gov-nasa-hdrl-data1/")
frID = "aia_0094"
myjson = fr.get_entry(frID)
start, stop = myjson['startDate'], myjson['stopDate']


file_registry1 = fr.request_file_registry(frID, start_date=start, stop_date=stop, overwrite=False)
# And convert that richer data to a list of files to process
filelist = file_registry1['key'].to_list()
now=time.time()
for i in range(10):
    results = process_fits_s3(filelist[i])
    print(results)
print(time.time()-now)

if daskMe:
    gateway = Gateway()
    options = gateway.cluster_options()
    options.worker_cores = w_cores
    options.worker_memory = w_memory

    cluster = gateway.new_cluster(options)
    client = cluster.get_client() # can also use 'client=Client(cluster)'
    cluster.adapt(minimum=1, maximum=n_workers)

    now=time.time()
    # simple version step 1, do it
    time_irrad = client.map(process_fits_s3, s3_files)
    time_irrad[0:4] # handy way to spot-check that the jobs were sent
    # simple version step 2, gather results
    all_data = client.gather(time_irrad)
    print(time.time()-now, len(all_data))
    # always shutdown
    cluster.shutdown()

    with open('SDO_test.pickle','wb') as fout:
        pickle.dump(all_data, fout)
