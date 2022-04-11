# OCR table extraction

## Description
Your client would like to extract data tables from the CRA ("Compte-Rendu d'Activite"). 

This type of document is complex, it is often a PDF document of many pages, with text but also elements of tables of figures that it would be interesting to compare to make predictions and evaluate the metrics against expectations.

To do this you will need to use OCR techniques to retrieve the data from the PDF files and then store this tabular data in a relational database management system (e.g. MySQL).

The client wants you to work in a iterative manner:

First test the feasibility of extracting tables of figures from a PDF file (the data file may be useful)
then the extraction of different tables in the same page of a PDF (the Test1 file could be a good basis).

Of course, remember to automate your code as much as possible.
