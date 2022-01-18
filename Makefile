clean_database:
	@rm -f database/*
	@echo "Erased database"
	@py3clean .
	@rm -rf __pycache__
	
clean_stats:
	@rm -f -r stats/*
	@mkdir stats/histograms
	@echo "Erased stats"
	
clean_index:
	@rm -f database/indexes.txt

generate:
	@python3 generator.py
	
test:
	@python3 main.py
