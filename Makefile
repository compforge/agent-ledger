.PHONY: fix lint test build

fix:
	$(MAKE) -C python fix
	$(MAKE) -C go fix

lint:
	$(MAKE) -C python lint
	$(MAKE) -C typescript lint
	$(MAKE) -C go lint

test:
	$(MAKE) -C python test
	$(MAKE) -C typescript test
	$(MAKE) -C go test

build:
	$(MAKE) -C python build
	$(MAKE) -C typescript build
	$(MAKE) -C go build
