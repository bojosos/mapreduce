#include "mr.h"

#include <string>
#include <cstdio>
#include <cstring>
#include <cstdlib>
#include <fstream>
#include <iostream>

#include <unordered_map>

#include "mr.h"
#include "Timer.h"

// https://github.com/bojosos/Crowny/blob/master/Crowny/Source/Crowny/Common/StringUtils.cpp
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

class __declspec(dllexport) WordCount : public MapTask // maybe don't even need dllexport
{
public:
	virtual void Map(const std::string_view& line, IntermediateEmitter emitter) override
	{
		std::vector<std::string> tokens = SplitString(line, " \t\n\r"); // split line into words
		for (const std::string& token : tokens)
		{
			if (!token.empty())
				emitter(token, 1);
		}
	}

	virtual void Reduce(const std::string& key, Getter getter, uint32_t partition, Emitter emitter) override
	{
		int count = 0;
		uint32_t value;

		while ((value = getter(key, partition)) != (uint32_t)-1)
			count ++;

		std::cout << "\"" << key << "\" " << count << " " << std::endl;
		emitter(key, count, partition);
	}
};

class __declspec(dllexport) Sort : public MapTask
{
	virtual void Map(const std::string_view& line, IntermediateEmitter emitter)  override
	{
		emitter(std::string(line), 0);
	}

	virtual void Reduce(const std::string& key, Getter getter, uint32_t partition, Emitter emitter) override
	{
		int count = 0;
		uint32_t value;
		while ((value = getter(key, partition)) != (uint32_t)-1)
			count += 1;
		emitter(key, value, partition);
		printf("%s\n", key.c_str());
	}
};

Timer timer;

extern "C" __declspec(dllexport) void* __cdecl CreateTask()
{
	timer.Start();
	return new WordCount();
	// return new Sort();
}

extern "C" __declspec(dllexport) void __cdecl EndTask()
{
	std::cout << "Task took: " << timer.ElapsedSeconds() << " seconds" << std::endl;
}
