Scraper for Flat Rental Prices
==============================
This is a small scraper for flat rental prices in Karlsruhe, Germany.

The data it produces can be visualized using [mietmap].


Usage
------------
Clone the repository:

    git clone https://github.com/CodeforKarlsruhe/mietmap-scraper.git
    cd mietmap-scraper

Create a [virtualenv] and activate it:

    virtualenv venv
    source venv/bin/activate

Install the dependencies:

    pip install -r requirements.txt

Run the scaper:

    python scrape.py

`--help` shows the available options. Log messages are written to `scrape.log`.


License
-------
Licensed under the MIT license, see the file `LICENSE`.


[mietmap]: https://github.com/CodeforKarlsruhe/mietmap
[virtualenv]: https://virtualenv.pypa.io/

