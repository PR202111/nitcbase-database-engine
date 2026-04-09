# ===== Build Mode =====
ifeq ($(mode),debug)
	CFLAGS := -g
	BUILD_DIR = ./build/debug
	TARGET = nitcbase-debug

else ifeq ($(mode),trace)
	CFLAGS := -DTRACE
	BUILD_DIR = ./build/trace
	TARGET = nitcbase-trace

else
	CFLAGS :=
	BUILD_DIR = ./build
	TARGET = nitcbase
endif

# ===== Directories =====
SUBDIR = FrontendInterface Frontend Algebra Schema BlockAccess BPlusTree Cache Buffer Disk_Class

# ===== Source Files =====
SRCS = main.cpp trace.cpp $(foreach fd,$(SUBDIR),$(wildcard $(fd)/*.cpp))

# ===== Object Files =====
OBJS = $(addprefix $(BUILD_DIR)/,$(SRCS:.cpp=.o))

# ===== Headers =====
HEADERS = $(wildcard define/*.h trace.h $(foreach fd,$(SUBDIR),$(fd)/*.h))

# ===== Build Target =====
$(TARGET): $(OBJS)
	g++ $(CFLAGS) -o $@ $(OBJS) -lreadline

# ===== Compile Rule =====
$(BUILD_DIR)/%.o: %.cpp $(HEADERS)
	@mkdir -p $(@D)
	g++ $(CFLAGS) -c $< -o $@

# ===== Clean =====
clean:
	rm -rf $(BUILD_DIR) $(TARGET)