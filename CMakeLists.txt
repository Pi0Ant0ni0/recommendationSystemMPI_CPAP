cmake_minimum_required(VERSION 3.27)
project(MPIrecommendationsystem)

set(CMAKE_CXX_STANDARD 14)
find_package(MPI REQUIRED)

add_executable(MPIrecommendationsystem
        "MPI Recommendation System/genRandomMatUI.cpp"
        "MPI Recommendation System/recommendationSystem.cpp"
        "MPI Recommendation System/recommendationSystem_serial.cpp"
        "MPI Recommendation System/recommendedMovies.cpp")
target_link_libraries(MPIrecommendationsystem PRIVATE MPI::MPI_C)


