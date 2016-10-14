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
MVN_OPTS="-e -pl $(subst $(SPACE),$(COMMA),$(strip $(MVN_PL)))"
MVN_REPO="$(HOME)/.m2/repository"

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

.PHONY: package
package:
	$(MVN) $(MVN_OPTS) package -DskipTests

.PHONY: install
install:
	$(MVN) $(MVN_OPTS) install -DskipTests

.PHONY: dummy
dummy:
	@echo 
