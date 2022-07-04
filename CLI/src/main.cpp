#include <iostream>
#include <cstring>
#include <string>
#include <filesystem>
#include <windows.h>

#include "MapReduce.h"
#include "Timer.h"

#include <fstream>

typedef MapTask* (__cdecl* CreateMapTaskFunc)();
typedef void (__cdecl *EndTaskFunction)();

std::vector<std::string> SplitString(const std::string_view& s, const std::string& separator)
{
	std::vector<std::string> output;

	std::string::size_type start = 0, end = s.find_first_of(separator);

	while ((end <= std::string::npos))
	{
		std::string_view tok = s.substr(start, end - start);
		if (!tok.empty())
			output.push_back(std::string(tok));

		if (end == std::string::npos)
			break;

		start = end + 1;
		end = s.find_first_of(separator, start);
	}

	return output;
}

#include <unordered_map>

int main(int argc, char** argv)
{
	int numThreads = std::thread::hardware_concurrency();

	std::vector<std::string> inputFiles;
	fs::path dllPath;
	uint32_t partitions = 0;
	bool mergeFiles = false;
	// TODO: Check if an argument is present before retrieving it
	for (int i = 1; i < argc; i++)
	{
		std::string arg = argv[i];
		if (arg == "-t")
		{
			try {
				numThreads = std::stoi(argv[i + 1]);
				i++;
			}
			catch (std::exception& e)
			{
				std::cout << "Invalid thread count" << std::endl;
				return 0;
			}
		}
		else if (arg == "-d")
		{
			try {
				dllPath = argv[i + 1];
				i++;
			}
			catch (std::exception& e)
			{
				std::cout << "Invalid thread count" << std::endl;
				return 0;
			}
		}
		else if (arg == "-p")
		{
			try {
				partitions = std::stoi(argv[i + 1]);
				i++;
			}
			catch (std::exception& e)
			{
				std::cout << "Invalid thread count" << std::endl;
				return 0;
			}
		}
		else if (arg == "-m")
			mergeFiles = true;
		else if (arg == "-h")
		{
			std::cout << "Usage\n";
			std::cout << "./mapreduce [options] file [files]\n";
			std::cout << "Option -d is required.\n";
			std::cout << "Options:\n";
			std::cout << "-d [dll path] to set the path to the dll with the map/reduce functions to use.\n";
			std::cout << "-p [partitions] to specify number of partitions. By default it uses the number of threads, so that every reducer with process the data passed from a single partitioner.\n";
			std::cout << "-t [threads] to specify a number of threads. By default std::thread::hardware_concurrency will be used." << std::endl;
			std::cout << "-m to merge files after reduce stage." << std::endl;
		}
		else
			inputFiles.push_back(arg);
	}
	for (const std::string& inputFile : inputFiles)
	{
		if (!fs::exists(inputFile))
		{
			std::cout << "File " << inputFile << " doesn't exist." << std::endl;
			return 0;
		}
	}
	if (!fs::exists(dllPath))
	{
		std::cout << "Dll file with map/reduce functions doesn't exist" << std::endl;;
		return 0;
	}

	// Please explain why I need absolute path
	HINSTANCE hProcId = LoadLibrary(fs::absolute(dllPath).c_str());
	// HINSTANCE hProcId = LoadLibrary(L"C:\\dev\\MapReduce\\bin\\Debug-windows-x86_64\\Sandbox\\Sandbox.dll");
	// HINSTANCE hProcId = LoadLibrary(L"C:\\dev\\MapReduce\\bin\\Release-windows-x86_64\\Sandbox\\Sandbox.dll");
	if (!hProcId)
	{
		std::cout << "Couldn't load dll." << std::endl;
		return 0;
	}
	std::cout << "Loaded " << dllPath << std::endl;

	CreateMapTaskFunc mapTaskCreateFunc = (CreateMapTaskFunc)GetProcAddress(hProcId, "CreateTask");
	if (mapTaskCreateFunc == nullptr)
	{
		std::cout << "Couldn't find CreateTask function" << std::endl;
		return 0;
	}
	MapTask* mapTask = mapTaskCreateFunc();

	if (partitions == 0)
		partitions = numThreads;
	std::cout << "Running with " << numThreads << " threads and " << partitions << " partitions." << std::endl;
	
	Job job;
	job.filenames = inputFiles;
	job.mapTask = mapTask;
	job.partitions = partitions;
	job.mapThreads = job.reduceThreads = numThreads;
	job.partitions = partitions;
	job.mergeFiles = mergeFiles;

	MapReduce mapReduce;
	mapReduce.Run(job);
	
	EndTaskFunction endTask = (EndTaskFunction)GetProcAddress(hProcId, "EndTask");
	if (endTask != nullptr)
		endTask();

	delete mapTask;

	return 0;
}