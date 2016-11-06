SPACE :=
SPACE +=
COMMA=,

MVN_PL=\
	!presto-docs \
	!presto-product-tests \
	!presto-server \
	!presto-server-rpm \
	!presto-testing-server-launcher \
	!presto-verifier

MVN=./mvnw
MVN_OPTS=-e -pl $(subst $(SPACE),$(COMMA),$(strip $(MVN_PL)))
MVN_REPO=$(HOME)/.m2/repository

.PHONY: all
all: package

.PHONY: clean
clean:
	if [ -d "$(MVN_REPO)/com/facebook/presto" ] ; then \
		find "$(MVN_REPO)/com/facebook/presto" \
			-name 'presto-*' \
			-type d \
			-maxdepth 1 \
			-not -name 'presto-root' \
			-not -name 'presto-spi' \
			-exec rm -r "{}" \; ; \
	fi
	if [ -d "$(MVN_REPO)/com/wrmsr/presto" ] ; then \
		find "$(MVN_REPO)/com/wrmsr/presto" \
			-name 'presto-*' \
			-type d \
			-maxdepth 1 \
			-exec rm -r "{}" \; ; \
	fi
	$(MVN) clean

.PHONY: test
test:
	$(MVN) $(MVN_OPTS) package

.PHONY: package
package:
	$(MVN) $(MVN_OPTS) package -DskipTests

.PHONY: fast
fast:
	$(MVN) $(MVN_OPTS) package -DskipTests \
		-Dair.check.skip-all=true \
		-Dair.check.skip-checkstyle=true \
		-Dair.check.skip-extended=true \
		-Dair.check.skip-license=true \
		-Denforcer.skip=true \
		-DignoreNonCompile=true \
		-Dlicense.skip=true \
		-Dmaven.test.skip=true \
		-Dmdep.analyze.skip=true \
		-Dmdep.skip=true

.PHONY: install
install:
	$(MVN) $(MVN_OPTS) install -DskipTests

.PHONY: dist
dist:
	$(MVN) \
		-pl presto-fusion-launcher \
		"exec:java" \
		-Dexec.mainClass=com.wrmsr.presto.launcher.packaging.Packager

.PHONY: pull-updates
pull-updates:
	git update-index -q --ignore-submodules --refresh
	git diff-files --quiet --ignore-submodules -- ; \
	if [ $$? -ne 0 ] ; then \
		echo >&2 ; \
		echo >&2 "Cannot update: you have unstaged changes." ; \
		git diff-files --name-status -r --ignore-submodules -- >&2 ; \
		echo >&2 ; \
		exit 1 ; \
	fi
	git diff-index --cached --quiet HEAD --ignore-submodules -- ; \
	if [ $$? -ne 0 ] ; then \
		echo >&2 ; \
		echo >&2 "Cannot update: your index contains uncommitted changes." ; \
		git diff-index --cached --name-status -r --ignore-submodules HEAD -- >&2 ; \
		echo >&2 ; \
		exit 1 ; \
	fi
	git pull upstream master --no-edit
	VERSION=$$(shell \
		egrep -o '^[ ]{4}<version>0\.[0-9]+-SNAPSHOT</version>' pom.xml | \
		head -n 1 | \
		egrep -o '0\.[0-9]+-SNAPSHOT' )
	if [ -z "$(VERSION)" ] ; then \
		echo >&2 "Failed to find version" ; \
		exit 1 ; \
	fi
	@echo VERSION=$(VERSION)
	find . -mindepth 2 -maxdepth 2 -name 'pom.xml' -type f | \
		xargs -n 1 sed -E -i '' \
			's/^([ ]{8}<version>)0\.[0-9]+-SNAPSHOT(<\/version>)/\1$(VERSION)\2/'
	find . -mindepth 2 -maxdepth 2 -name 'pom.xml' -type f | \
		xargs -n 1 git add
	git diff-index --cached --quiet HEAD --ignore-submodules -- ; \
	if [ $$? -ne 0 ] ; then \
		set -e ; \
		git commit -m "Update to version $(VERSION)" ; \
	fi

.PHONY: update
update: | pull-updates clean install

.PHONY: dummy
dummy:
	@echo 
