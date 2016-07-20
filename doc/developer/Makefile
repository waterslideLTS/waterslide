# Pre-configuration

# Flags for gcc
SOURCES = waterslide_programming_guide.tex waterslide_refsheet.tex

# All C files as sources, and chop off the .c for targets
MODULES = $(patsubst %.tex, %.pdf, $(SOURCES))

all: $(MODULES)

# All targets without an extension depend on their .c files
%.pdf: %.tex
	pdflatex $<
	pdflatex $<
clean:
	@rm -rf *.log *.aux 2> /dev/null

