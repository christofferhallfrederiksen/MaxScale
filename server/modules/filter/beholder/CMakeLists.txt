set(CMAKE_CXX_FLAGS "-std=c++11 ${CMAKE_CXX_FLAGS}")
set(CMAKE_CXX_FLAGS_DEBUG "-std=c++11 ${CMAKE_CXX_FLAGS_DEBUG}")
set(CMAKE_CXX_FLAGS_RELEASE "-std=c++11 ${CMAKE_CXX_FLAGS_RELEASE}")
set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=c++11 ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")

add_library(beholder SHARED beholder.cc)
target_link_libraries(beholder maxscale-common)
set_target_properties(beholder PROPERTIES VERSION "1.0.0")
install_module(beholder core)
