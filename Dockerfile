FROM python:3.10.0

WORKDIR /app

EXPOSE 8001

COPY requirements.txt requirements.txt

RUN pip install --upgrade pip && pip install --upgrade setuptools
RUN pip install -r requirements.txt

COPY . .

# CMD "python3 main.py"
CMD ["python", "main.py"]