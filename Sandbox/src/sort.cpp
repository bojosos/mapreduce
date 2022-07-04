#include <iostream>

#include "mr.h"
#include "Timer.h"

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

/*
Timer timer;

extern "C" __declspec(dllexport) void* __cdecl CreateTask()
{
	timer.Start();
	return new Sort();
}

extern "C" __declspec(dllexport) void __cdecl EndTask()
{
	std::cout << "Task took: " << timer.ElapsedSeconds() << " seconds" << std::endl;
}
*/