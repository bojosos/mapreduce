#pragma once

#include <string>
#include <filesystem>
#include <functional>

namespace fs = std::filesystem;

// typedef uint32_t (*Getter)(const std::string& key, uint32_t partition);
// typedef void (*Emitter)(const std::string& key, uint32_t value);
using IntermediateEmitter = std::function<void(const std::string&, uint32_t)>;
using Getter = std::function<uint32_t(const std::string&, uint32_t partition)>;
using Emitter = std::function<void(const std::string&, uint32_t value, uint32_t partition)>;

class MapTask
{
public:
	MapTask() = default;
	virtual void Map(const std::string_view& file, IntermediateEmitter  emitter) = 0;
	virtual void Reduce(const std::string& key, Getter getter, uint32_t parition, Emitter emitter) = 0;
};