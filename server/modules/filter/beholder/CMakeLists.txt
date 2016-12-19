if (JANSSON_FOUND)

ExternalProject_Add(hiredis
  PREFIX hiredis
  GIT_REPOSITORY "https://github.com/redis/hiredis.git"
  CONFIGURE_COMMAND ""
  BUILD_COMMAND make -C ${CMAKE_CURRENT_BINARY_DIR}/hiredis/src/hiredis/
  INSTALL_COMMAND make -C ${CMAKE_CURRENT_BINARY_DIR}/hiredis/src/hiredis/ install PREFIX=/hiredis DESTDIR=${CMAKE_CURRENT_BINARY_DIR}
  UPDATE_COMMAND "")

  include_directories(${CMAKE_CURRENT_BINARY_DIR}/hiredis/include/)
  set(HIREDIS_LIBRARIES "${CMAKE_CURRENT_BINARY_DIR}/hiredis/lib/libhiredis.a")

  set(CMAKE_CXX_FLAGS "-std=c++11 ${CMAKE_CXX_FLAGS}")
  set(CMAKE_CXX_FLAGS_DEBUG "-std=c++11 ${CMAKE_CXX_FLAGS_DEBUG}")
  set(CMAKE_CXX_FLAGS_RELEASE "-std=c++11 ${CMAKE_CXX_FLAGS_RELEASE}")
  set(CMAKE_CXX_FLAGS_RELWITHDEBINFO "-std=c++11 ${CMAKE_CXX_FLAGS_RELWITHDEBINFO}")

  add_library(beholder SHARED beholder.cc datapoint.cc relay.cc file_relay.cc redis_relay.cc)
  target_link_libraries(beholder maxscale-common ${JANSSON_LIBRARIES} ${HIREDIS_LIBRARIES})
  set_target_properties(beholder PROPERTIES VERSION "1.0.0")
  install_module(beholder core)
endif()
