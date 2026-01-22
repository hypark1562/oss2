
.PHONY: install format lint run clean


install:
	pip install -r requirements.txt
	pre-commit install


format:
	black .
	isort .


lint:
	pylint etl/ utils/ main.py dashboard.py


run:
	python main.py


dashboard:
	streamlit run dashboard.py


clean:
	rm -rf __pycache__
	rm -rf logs/
	rm -rf data/
