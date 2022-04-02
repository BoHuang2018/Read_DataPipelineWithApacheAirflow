FROM python:3.9-slim

COPY requirements.txt /tmp/requirements.txt
RUN pip install -r /tmp/requirements.txt && rm -f /tmp/requirements.txt

# Scrape ratings record http://files.grouplens.org/datasets/movielens/ml-25m.zip, for simulating scraping API
COPY app.py fetch_ratings.py ./
RUN python fetch_ratings.py --output_path ratings.csv

EXPOSE 5001

ENTRYPOINT ["python"]
CMD ["/app.py"]