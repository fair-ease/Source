## TO DO: Translate in english

SOURCE

nella cartella /scripts sono presenti dei bash file per la creazione delle immagini docker

SPHINX

partendo dall'immagine ufficiale di sphinx e' possibile creare un'altra immagine includendo altri temi oltre a quelli di default

l'elenco dei temi e' contenuto nel file requirements_sphinx.txt

per limitare la lista dei temi da installare possono essere cancellate o commentate delle righe nel file requirements_sphinx.txt

per creare una nuova immagine docker di sphinx:

.. code-block:: bash

  # esempio:
  docker build --no-cache -f Dockerfile_base --label sphinx_latest --tag sphinx:latest .
  
  # or
  docker build --no-cache -f Dockerfile_latex --label sphinx-latex_latest --tag sphinx-latex:latest .


il tema dovra' essere configurato nel file ``conf.py``. Verificare la giusta configurazione per lo specifico tema

`Link sphinx themes <https://sphinx-themes.org/#theme-sphinx-rtd-theme>`

per compilare il codice html bisognera' utilizzare la nuova immagine

.. code-block:: bash

  # esempio
  docker run --rm -v $(pwd)/docs:/docs sphinx:latest make html
  

