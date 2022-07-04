#pragma once

#include <mutex>
#include <vector>
#include <string>

#include <fstream>

#include "mr.h"

using Lock = std::lock_guard<std::mutex>;

const uint64_t FileSize = 1048576;

enum class InputFormat
{
	// Default input format, will split on new line
	Text
};

struct Job
{
	InputFormat inputFormat = InputFormat::Text;
	uint32_t reduceThreads = 0;
	uint32_t mapThreads = 0;
	uint32_t partitions = 0;
	std::vector<std::string> filenames;
	MapTask* mapTask = nullptr;
	bool mergeFiles = true;
	fs::path outputFilename;
};

class MapReduce
{
	struct Node
	{
		std::string key;
		uint32_t value;
		int processed;
		Node* next;
	};

	struct Partition
	{
		Node* head = nullptr;
		std::mutex mutex;
		std::ofstream stream;
	};

public:
	using LineList = std::vector<std::pair<uint32_t, uint32_t>>;
	void EmitIntermediate(const std::string& key, uint32_t count);
	void Run(const Job& job);
	void Emit(const std::string& key, uint32_t value, uint32_t partition);
	
private:
	Node* Merge(Node* a, Node* b);
	void MoveNode(Node** destRef, Node** sourceRef);
	void Split(Node* src, Node** a, Node** b);
	void MergeSort(Node** nodes);
	void mapFunction(uint32_t start, uint32_t end);
	void reduceFunction();
	void Backup(Node* node, uint32_t partitionIdx);
	uint64_t ParitionFunc(const std::string& key);
	uint32_t Getter(const std::string& key, uint32_t partition);

	uint32_t m_NextPartition = 0;
	InputFormat m_InputFormat;
	std::string m_InputBuffer;
	LineList m_Lines;
	MapTask* m_MapTask;
	std::mutex m_PartitionMutex;
	std::vector<std::string> m_Files;
	Partition* m_Partitions;
	Partition* m_BackupPartitions;
	uint64_t m_NumPartitions = 0;
	bool* m_IsNextKeyDifferent;
};