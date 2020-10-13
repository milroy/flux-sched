AM_CXXFLAGS = \
    $(WARNING_CXXFLAGS) \
    -Wno-unused-local-typedefs \
    -Wno-deprecated-declarations \
    -Wno-unused-variable \
    -Wno-error \
    $(CODE_COVERAGE_CXXFLAGS)

AM_LDFLAGS = $(CODE_COVERAGE_LDFLAGS) -lpython3.6m -lpthread -ldl  -lutil -lm  -Xlinker -export-dynamic# `python3-config --ldflags`

AM_CPPFLAGS = -I$(top_srcdir) $(CZMQ_CFLAGS) $(FLUX_CORE_CFLAGS) \
          $(BOOST_CPPFLAGS) `python3-config --cflags`

fluxmod_LTLIBRARIES = resource.la

#
# resource service module
#
resource_la_SOURCES = \
    resource_match.cpp
resource_la_CXXFLAGS = \
    $(AM_CXXFLAGS) \
    $(JOBSPEC_CFLAGS) \
    $(FLUX_CORE_CFLAGS)
resource_la_LIBADD = \
    ../libresource.la \
    $(FLUX_CORE_LIBS) \
    $(DL_LIBS) \
    $(HWLOC_LIBS) \
    $(UUID_LIBS) \
    $(JANSSON_LIBS) \
    $(CZMQ_LIBS) \
    $(BOOST_LDFLAGS)
resource_la_LDFLAGS = \
    $(AM_LDFLAGS) \
    $(fluxmod_ldflags) -module

