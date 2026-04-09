ifeq ($(mode),debug)
	CFLAGS := -g
	BUILD_DIR = ./build/debug
	TARGET = nitcbase-debug
else
	TARGET = nitcbase
	BUILD_DIR = ./build
endif

SUBDIR = FrontendInterface Frontend Algebra Schema BlockAccess BPlusTree Cache Buffer Disk_Class

HEADERS = $(wildcard define/*.h $(foreach fd, $(SUBDIR), $(fd)/*.h))
SRCS = $(wildcard main.cpp $(foreach fd, $(SUBDIR), $(fd)/*.cpp))
OBJS = $(addprefix $(BUILD_DIR)/, $(SRCS:cpp=o))

$(TARGET): $(OBJS)
	g++ $(CFLAGS) -o $@ $(OBJS) -lreadline

$(BUILD_DIR)/%.o: %.cpp $(HEADERS)
	mkdir -p $(@D)
	g++ $(CFLAGS) -o $@ -c $<

clean:
	rm -rf $(BUILD_DIR)/*

# ===== Build Mode =====
# ifeq ($(mode),debug)
# 	CFLAGS := -g -DTRACE
# 	BUILD_DIR = ./build/debug
# 	TARGET = nitcbase-debug
# else
# 	CFLAGS :=
# 	BUILD_DIR = ./build
# 	TARGET = nitcbase
# endif

# # ===== Project Structure =====
# SUBDIR = FrontendInterface Frontend Algebra Schema BlockAccess BPlusTree Cache Buffer Disk_Class

# SRCS = $(wildcard main.cpp $(foreach fd,$(SUBDIR),$(fd)/*.cpp))
# OBJS = $(addprefix $(BUILD_DIR)/,$(SRCS:.cpp=.o))
# DEPS = $(OBJS:.o=.d)

# # ===== Build Target =====
# $(TARGET): $(OBJS)
# 	g++ $(CFLAGS) -o $@ $(OBJS) -lreadline

# # ===== Compile Rule (AUTO DEPENDENCY GENERATION) =====
# $(BUILD_DIR)/%.o: %.cpp
# 	@mkdir -p $(@D)
# 	g++ $(CFLAGS) -MMD -MP -c $< -o $@

# # ===== Include dependency files =====
# -include $(DEPS)

# # ===== Print File Dependency Tree =====
# print-deps:
# 	@echo "==== FILE DEPENDENCIES ===="
# 	@find $(BUILD_DIR) -name "*.d" | sort | while read f; do \
# 		echo "\n---- $$f ----"; \
# 		cat $$f; \
# 	done

# # ===== Function Call Graph =====
# callgraph:
# 	cflow $(SRCS)

# # ===== Clean =====
# clean:
# 	rm -rf $(BUILD_DIR)/* $(TARGET)