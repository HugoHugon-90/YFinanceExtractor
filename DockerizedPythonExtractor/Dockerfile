FROM python:3.9
COPY requirements.txt /requirements.txt
WORKDIR /
RUN pip install -r ./requirements.txt
COPY yfinance-extractor.py /yfinance-extractor.py
EXPOSE 5050
CMD python3 ./yfinance-extractor.py
