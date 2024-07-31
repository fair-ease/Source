# Configuration file for the Sphinx documentation builder.
#
# For the full list of built-in configuration values, see the documentation:
# https://www.sphinx-doc.org/en/master/usage/configuration.html

# -- Project information -----------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#project-information

project = 'SOURCE'
copyright = '2024, Istituto Nazionale di Geofisica e Vulcanologia'
author = 'Paolo Frizzera'
version = '1.4.4'
release = '0.0.0.1'

# -- General configuration ---------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#general-configuration

#extensions = []
extensions = [
    'sphinx_rtd_theme',
    'sphinx_simplepdf',
    'rst2pdf.pdfbuilder',
    #'sphinx_pdf_generate',
    #'sphinx_immaterial',
]

# -- Options for HTML output -------------------------------------------------
# https://www.sphinx-doc.org/en/master/usage/configuration.html#options-for-html-output

#html_theme = 'alabaster'
#html_theme = 'sphinx_rtd_theme'
#html_theme = 'furo'
#html_theme = 'sphinx_book_theme'
#html_theme = 'pydata_sphinx_theme'
#html_theme = 'press'
#html_theme = 'piccolo_theme'
#html_theme = 'insegel'
#html_theme = 'cloud'
#html_theme = 'conestack'
#html_theme = 'sphinx_documatt_theme'
#html_theme = 'groundwork'
#html_theme = 'sphinx_nefertiti'
#html_theme = 'python_docs_theme'
html_theme = 'bizstyle'
#html_theme = 'agogo'
#html_theme = 'classic'
#html_theme = 'haiku'
#html_theme = 'nature'
#html_theme = 'pyramid'
#html_theme = 'scrolls'
#html_theme = 'sphinxdoc'
#html_theme = 'traditional'

##### insipid theme
#html_theme = 'insipid'
#html_permalinks_icon = '§'

##### ADC theme
#import sphinx_adc_theme
#html_theme = 'sphinx_adc_theme'
#html_theme_path = [sphinx_adc_theme.get_html_theme_path()]

##### awesome theme
#html_theme = 'sphinxawesome_theme'
#html_permalinks_icon = '<span>#</span>'

##### better theme
#import better
#html_theme = 'better'
#html_theme_path = [better.better_theme_path]

##### bootstrap theme
#import sphinx_bootstrap_theme
#html_theme = 'bootstrap'
#html_theme_path = sphinx_bootstrap_theme.get_html_theme_path()

##### maisie theme
#import maisie_sphinx_theme
#extensions.append("maisie_sphinx_theme")
#html_theme = 'maisie_sphinx_theme'
#html_theme_path = maisie_sphinx_theme.html_theme_path()

##### nameko theme
#import sphinx_nameko_theme
#html_theme = 'nameko'
#html_theme_path = [sphinx_nameko_theme.get_html_theme_path()]

##### PD theme
#import sphinx_theme_pd
#html_theme = 'sphinx_theme_pd'
#html_theme_path = [sphinx_theme_pd.get_html_theme_path()]

##### PDJ theme
#import sphinx_pdj_theme
#html_theme = 'sphinx_pdj_theme'
#html_theme_path = [sphinx_pdj_theme.get_html_theme_path()]

##### readable theme
#import sphinx_readable_theme
#html_theme = 'readable'
#html_theme_path = [sphinx_readable_theme.get_html_theme_path()]

##### mozilla theme
#import os
#import mozilla_sphinx_theme
#html_theme = 'mozilla'
#html_theme_path = [os.path.dirname(mozilla_sphinx_theme.__file__)]

##### solar theme
#import solar_theme
#html_theme = 'solar_theme'
#html_theme_path = [solar_theme.theme_path]

##### wagtail theme
#extensions.append("sphinx_wagtail_theme")
#html_theme = 'sphinx_wagtail_theme'

##### zerovm theme
#import zerovm_sphinx_theme
#html_theme = 'zerovm'
#html_theme_path = [zerovm_sphinx_theme.theme_path]

##### tibas theme
#import tibas.tt
#import alabaster
#html_theme = 'tt'
#html_theme_path = [tibas.tt.get_path(), alabaster.get_path()]

##### sphinx_material theme - for sphinx_pdf_generate
#html_theme = 'sphinx_material'
# Set link name generated in the top bar.
#html_title = 'Project Title'

# Material theme options (see theme.conf for more information)
#html_theme_options = {
#    # Set the name of the project to appear in the navigation.
#    'nav_title': 'Project Name',
#
#    # Set you GA account ID to enable tracking
#    ##'google_analytics_account': 'UA-XXXXX',
#
#    # Specify a base_url used to generate sitemap.xml. If not
#    # specified, then no sitemap will be built.
#    'base_url': 'https://project.github.io/project',
#
#    # Set the color and the accent color
#    'color_primary': 'blue',
#    'color_accent': 'light-blue',
#
#    # Set the repo location to get a badge with stats
#    'repo_url': 'https://github.com/project/project/',
#    'repo_name': 'Project',
#
#    # Visible levels of the global TOC; -1 means unlimited
#    'globaltoc_depth': 3,
#    # If False, expand all TOC entries
#    'globaltoc_collapse': False,
#    # If True, show hidden TOC entries
#    'globaltoc_includehidden': False,
#}

##### sphinx_immaterial theme - for sphinx_pdf_generate
#html_theme = 'sphinx_immaterial'



templates_path = ['_templates']
html_static_path = ['_static']
exclude_patterns = []

language = 'en'


########## latex
#latex_engine = 'pdflatex'
##latex_engine = 'xelatex'
#latex_elements = {
#    'papersize': 'a4paper',
#    'pointsize': '12pt',
#    'pxunit': '0.75bp',
#    'sphinxsetup': 'verbatimwrapslines=true, verbatimforcewraps=true',
#    'fncychap': r'\setlength{\headheight}{14.49998pt}',
#}
##latex_documents = [
##    ('index','oceano-dev server.pdf','','','')
##]

########## pdf_documents
#pdf_documents = [('index', u'SOURCE', u'SOURCE documentation', u'geofrizz'),]

########## simple pdf
#simplepdf_vars = {
#    'primary': '#FA2323',
#    'cover-overlay': 'rgba(250, 35, 35, 0.5)',
#    'links': '#FF3333',
#}

########## Sphinx-PDF-Generate
## Sphinx-PDF-Generate global options
#pdfgen_site_url = "http://ingv.it"
#pdfgen_author = "Sphinx-PDF Generate"
#pdfgen_copyright = "2023, Sphinx-PDF Generate"
#pdfgen_disclaimer = "Disclaimer: Content can change at anytime and best to refer to website for latest information."
#pdfgen_cover = True
#pdfgen_cover_title = "Sphinx-PDF Generate"
#pdfgen_toc = True
#pdfgen_toc_numbering = True
#pdfgen_toc_title = "Table of Contents"
#pdfgen_toc_level = 4





