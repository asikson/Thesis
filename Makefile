clean:
	@rm -f database/*
	@echo "Erased database"
	@py3clean .
	@rm -rf __pycache__

generate:
	@python3 generator.py
