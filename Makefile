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
	if [ -d "$(MVN_REPO)/com/facebook/presto" ]; then \
		find "$(MVN_REPO)/com/facebook/presto" \
			-name 'presto-*' \
			-type d \
			-maxdepth 1 \
			-not -name 'presto-root' \
			-not -name 'presto-spi' \
			-exec rm -r "{}" \; ; \
	fi
	if [ -d "$(MVN_REPO)/com/wrmsr/presto" ]; then \
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
	$(MVN) -pl presto-fusion-launcher "exec:java" -Dexec.mainClass=com.wrmsr.presto.launcher.packaging.Packager

.PHONY: dummy
dummy:
	@echo 
