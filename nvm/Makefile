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
	cd $(BUILD_DIR) && make -j$(expr `nproc` - 1)

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
	cd .. && make release -j$(expr `nproc` - 1)

test-clean:
	@sudo rm -r /opt/rocks/test || true
	@sudo mkdir -p /opt/rocks/test || true
	@sudo chown -R `whoami`:`whoami` /opt/rocks || true
	@rm /tmp/test-*-*.log || true

test-01-00:
	./$(BUILD_DIR)/env_nvm_t01 0 2>&1 | tee -a /tmp/test-01-00.log
	cat /opt/rocks/nvm.meta
	cat /opt/rocks/test/testfile.log.meta
	nvm_addr from_hex /dev/nvme0n1 `tail -n +3 /opt/rocks/test/testfile.log.meta | cut -f 1 -d " "`

test-01-01:
	./$(BUILD_DIR)/env_nvm_t01 1  2>&1 | tee -a /tmp/test-01-01.log
	cat /opt/rocks/nvm.meta
	cat /opt/rocks/test/testfile.log.meta
	nvm_addr from_hex /dev/nvme0n1 `tail -n +3 /opt/rocks/test/testfile.log.meta | cut -f 1 -d " "`

# nruns nkvs do_delete disableWAL
test-02-00:
	./$(BUILD_DIR)/env_nvm_t02 3 50000 0 1 2>&1 | tee -a /tmp/test-02-00.log

test-02-01:
	./$(BUILD_DIR)/env_nvm_t02 3 50000 1 1 2>&1 | tee -a /tmp/test-02-01.log

bench-clean:
	@sudo rm -r /opt/rocks/hw || true
	@sudo rm -r /opt/rocks/fs || true
	@sudo mkdir -p /opt/rocks/hw || true
	@sudo mkdir -p /opt/rocks/fs || true
	@sudo chown -R `whoami`:`whoami` /opt/rocks || true
	@rm /tmp/bench*.log || true

bench-hw-fill:
	./scripts/run.sh ./scripts/env_shared.sh ./scripts/env_hw.sh fillseq 0 2>&1 | tee -a /tmp/bench-hw-fill.log

bench-hw-read:
	./scripts/run.sh ./scripts/env_shared.sh ./scripts/env_hw.sh readseq 1 2>&1 | tee -a /tmp/bench-hw-read.log

bench-fs-fill:
	./scripts/run.sh ./scripts/env_shared.sh ./scripts/env_fs.sh fillseq 0 2>&1 | tee -a /tmp/bench-fs-fill.log

bench-fs-read:
	./scripts/run.sh ./scripts/env_shared.sh ./scripts/env_fs.sh readseq 1 2>&1 | tee -a /tmp/bench-fs-read.log
