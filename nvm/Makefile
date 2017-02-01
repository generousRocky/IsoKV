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
	@sudo rm -r /opt/rtest || true
	@sudo mkdir -p /opt/rtest/db || true
	@sudo chown -R `whoami`:`whoami` /opt/rtest || true
	@rm /tmp/t0*.log || true

t01_00:
	./$(BUILD_DIR)/env_nvm_t01 0 2>&1 | tee -a /tmp/t01_00.log
	cat /opt/rtest/nvm.meta
	cat /opt/rtest/testfile.log.meta
	nvm_addr from_hex /dev/nvme0n1 `tail -n +3 /opt/rtest/testfile.log.meta | cut -f 1 -d " "`

t01_01:
	./$(BUILD_DIR)/env_nvm_t01 1  2>&1 | tee -a /tmp/t01_01.log
	cat /opt/rtest/nvm.meta
	cat /opt/rtest/testfile.log.meta
	nvm_addr from_hex /dev/nvme0n1 `tail -n +3 /opt/rtest/testfile.log.meta | cut -f 1 -d " "`

# nruns nkvs do_delete disableWAL
t02_00:
	./$(BUILD_DIR)/env_nvm_t02 3 50000 0 1 2>&1 | tee -a /tmp/t02_00.log

t02_01:
	./$(BUILD_DIR)/env_nvm_t02 3 50000 1 1 2>&1 | tee -a /tmp/t02_01.log

clean-bench:
	@sudo rm -r /opt/rbench || true
	@sudo mkdir -p /opt/rbench/db || true
	@sudo chown -R `whoami`:`whoami` /opt/rbench || true
	@rm /tmp/b0*.log || true

b01:
	./scripts/nvmdb_bench.sh ../ /opt/rbench 2>&1 | tee -a /tmp/b01.log

b02:
	./scripts/nvmdb_bench16MB.sh ../ /opt/rbench 2>&1 | tee -a /tmp/b02.log
