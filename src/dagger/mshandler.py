"""
Module to handle MS data, meta-data, splitting and tarring
"""

class MSHandler():
    """
    Handle Measurement Set v2 data & metadata parsing, selection, and
    processing.
    """

    def __init__(self, MS, SPWs, verbose):
        self.msfile = MS
        self.spwlist = SPWs
        self.nspw = len(self.spwlist)
        self.tarnames = []
        self.verbose = verbose

    def get_ms_info(self) -> (int, list):
        """
        Get the number of spectral windows and channels in the MS file.
        
        Parameters:
        None
        
        Returns:
        tuple: A tuple containing the number of spectral windows and channels per spw
        """
        msmd.open(self.msfile)
        nchan = []
        for spw in self.spwlist:
            nchan.append(msmd.nchan(spw))
        msmd.close()
        
        return len(self.spwlist), nchan


    def get_spw_selections(self, nchan:list, njob:int) -> list:
        """
        Generate a list of spectral window selections based on the number of jobs requested.
        
        Parameters:
        nchan (list): List of number of channels per spectral window.
        njob (int): Number of jobs to submit.
        
        Returns:
        list: A list of spectral window selections for each job.
        """
        total_chans = sum(nchan)
        
        chan_chunk  = total_chans // njob
        
        spw_selections = []
        
        for spw in range(self.nspw):
            for chan in range(0, nchan[spw], chan_chunk):
                spw_selections.append(f"{self.spwlist[spw]}:{chan}~{min(chan + chan_chunk - 1, nchan[spw] - 1)}")
        
        return spw_selections   


    @staticmethod
    def should_we_clobber(filepath:str, clobber:bool):
        """
        Check if a file/directory exists, and wipes it if the user has
        requested it.

        Inputs:
        filepath, str : Full path of file to clobber
        clobber, bool : Should the file be wiped?

        Returns:
        None
        """

        if not clobber and os.path.exists(filepath):
            logger.warn(f"Path {filepath} exists and user does not want to clobber. Doing nothing.")
            return False
        elif clobber and os.path.exists(filepath):
            logging.warn(f"Clobbering {filepath}")
            if os.path.isdir(filepath):
                shutil.rmtree(filepath)
            elif os.path.isfile(filepath):
                os.remove(filepath)

            return True


    def split_ms(self, spw_selections:list, output_dir:str, clobber_ms:bool=False, clobber_tar:bool=False):
        """
        Split the Measurement Set into smaller pieces based on the spectral window selections.
        
        Parameters:
        spw_selections (list): List of SPW selection strings
        output_dir (str): Directory to save the split MS files.
        clobber_ms (bool): Whether to overwrite existing MS files.
        clobber_tar (bool): Whether to overwrite existing tar files.
        """

        # Re-initialize, in case anything else over-writes it
        self.tarnames = []

        if not os.path.exists(output_dir):
            os.makedirs(output_dir)
        
        for ii, spw in enumerate(spw_selections):
            output_ms = f"{output_dir}/split_ms_{ii}.ms"
            if os.path.exists(output_ms) and not clobber_ms:
                print(f"Output file {output_ms} already exists. Skipping split for SPW {spw}.")
                continue

            ret = self.should_we_clobber(output_ms, clobber_ms)
            if self.verbose:
                logging.info(f"Splitting MS for SPW {spw} into {output_ms}")
            if ret:
                split(vis=self.msfile, outputvis=output_ms, spw=spw)
            
            tarname = f"{output_ms}.tar.gz"
            ret = self.should_we_clobber(tarname, clobber_tar)
            if self.verbose:
                logging.info(f"Tarring up MS {output_ms} into {tarname}")

            if ret:
                with tarfile.open(tarname, "w:gz") as tar:
                    tar.add(output_ms, arcname=os.path.basename(output_ms))

                if self.verbose:
                    logging.info(f"Created tarball {tarname} for {output_ms}")

            self.tarnames.append(tarname)

        return self.tarnames


