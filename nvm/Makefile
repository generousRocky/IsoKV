BUILD_TYPE?=Release
BUILD_DIR?=build

default: configure make

debug:
	$(eval BUILD_TYPE := Debug)

cmake_check:
	@cmake --version || (echo "\n** Please install 'cmake' **\n" && exit 1)

configure: cmake_check
	mkdir -p $(BUILD_DIR)
	cd $(BUILD_DIR) && cmake -DCMAKE_BUILD_TYPE=$(BUILD_TYPE) ..
	@echo "Modify build configuration in '$(BUILD_DIR)'"

make:
	cd $(BUILD_DIR) && make

install:
	cd $(BUILD_DIR) && make install

clean:
	rm -r $(BUILD_DIR) || true
	rm tags || true

all: clean default install

tags:
	ctags * -R src
	ctags -R --c++-kinds=+p --fields=+iaS --extra=+q /home/safl/git/rocksdb

dev: clean configure make


test-clean:
	rm -r /tmp/testdb || true

test-run:
	./$(BUILD_DIR)/env_nvm_test 10 5000 1 0

rocks:
	cd .. && make release -j$(nproc)

bench:
	rm -r /tmp/testdb || true
	./nvmdb_bench.sh ../ /tmp/jazz > /tmp/jazz.log
