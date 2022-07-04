# MapReduce

Final project for the Parallel Programming course at FMI.

## Instructions
After downloading run premake\premake5.exe vs2022. Then build the Sandbox project. It will generate a .dll file that can be used as input for CLI. Run the CLI project with -d [path to dll] [files]

There are 3 projects:
- CLI - command line tool for running map-reduce operations
    - -d [dllPath] specify dll
    - -m merge files 
    - -t number of threads
    - everything else will be interpreted as file names

- MapReduce - contains the map-reduce library
- Sandbox - contains 2 example tasks (sort and wordcount)

To create a new task implement the Map and Reduce functions the MapTask interface provides.
Then the task can be run either directly by creating a job, a MapReduce instance and calling run with the job or compiled into dll and ran with the CLI tool(you have to create a CreateTask method which returns a pointer to an instance of the task you want to run).

By default it will use the value of std::thread::hardware_concurrency for number of map/reduce threads and partition count.

## Output
part_[partition number] - output from each partition
If option mergeFiles is set to true all of the files will be merged into one
Expected wordcount output with words.txt ["abcdef", "314513", "8484", "tomato", "banana", "51523", "88888"] -  [14463, 14099, 14123, 14385, 14360, 14409, 14161]

### Slower even on large data sets.
