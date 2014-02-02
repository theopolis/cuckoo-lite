Cuckoo Sandbox *Lite*
=====================
This is a *lite* version of `Cuckoo sandbox <http://www.cuckoosandbox.org/>`_. 
Meaning, there are no dynamic analysis machines/guests, no machinery or analyzers. 
Samples are only processed, then reported on. 

Requirements
------------
Follow **most** of the documentation for Cuckoo, this is designed to enable the
Django web interface and main Cuckoo scheduling loop.

- Django (easy_install -U -O2 Django)
- mongodb (apt-get install python-pymogodb mongodb)
- sqlalchemy (pip install sqlalchemy)
- ssdeep and pydeep (apt-get install libfuzzy-dev && pip install pydeep)
- python-pefile