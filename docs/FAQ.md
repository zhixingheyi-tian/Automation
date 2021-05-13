This page contains the information for frequently asked questions. It also contains possible solutions for typical problems you may encountered for different environment.

## Error when running setup-cluster.sh
You may encounter error when running setup-cluster.sh on an old system with different version of Python packages installed. The error looks like the following:

    Traceback (most recent call last):  
      File "/usr/lib64/python2.7/site-packages/cryptography/hazmat/backends/openssl/ciphers.py", line 131, in update_into "unsigned char *", self._backend._ffi.from_buffer(buf)
      TypeError: from_buffer() cannot return the address of the raw string within a str or unicode or bytearray object
  
  When you encountered this error. You need to remove the all the existing Python packages using the following commands:
  
  pip freeze | xargs pip uninstall -y
  
  and run setup-env.sh again.
