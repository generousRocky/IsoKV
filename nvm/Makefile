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

.PHONY: tags
tags:
	ctags * -R .
	ctags -R --c++-kinds=+p --fields=+iaS --extra=+q ../

dev: clean configure make

rocks:
	cd .. && make release -j$(nproc)

clean-test:
	sudo rm -r /opt/rtest || true
	sudo mkdir -p /opt/rtest/db
	sudo chown -R `whoami`:`whoami` /opt/rtest

t01_0:
	./$(BUILD_DIR)/env_nvm_t01 0 > /tmp/jazz.log
	cat /opt/rtest/nvm.meta
	cat /opt/rtest/testfile.log.meta
	nvm_addr fmt_p nvme0n1 `tail -n +3 /opt/rtest/testfile.log.meta | cut -f 1 -d " "`

t01_1:
	./$(BUILD_DIR)/env_nvm_t01 1 > /tmp/jazz.log
	cat /opt/rtest/nvm.meta
	cat /opt/rtest/testfile.log.meta
	nvm_addr fmt_p nvme0n1 `tail -n +3 /opt/rtest/testfile.log.meta | cut -f 1 -d " "`

t02:
	./$(BUILD_DIR)/env_nvm_t02 3 50000 1 0 > /tmp/jazz.log

clean-bench:
	rm -r /opt/rtest/db || true
	rm /tmp/jazz.log || true

b01:
	./nvmdb_bench.sh ../ /opt/rtest/db 2>&1 | tee -a /tmp/jazz.log

b02:
	./nvmdb_bench16MB.sh ../ /opt/rtest/db > /tmp/jazz16MB.log
