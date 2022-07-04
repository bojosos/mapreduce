#include "MapReduce.h"

#include "Timer.h"
#include "Utils.h"

#include <thread>
#include <iostream>
#include <fstream>

void MapReduce::mapFunction(uint32_t start, uint32_t end)
{
	for (uint32_t i = start; i < end; i++)
	{
		auto& line = m_Lines[i];
		m_MapTask->Map(std::string_view(m_InputBuffer).substr(line.first, line.second - line.first + 1), [&](const std::string& key, uint32_t value) { EmitIntermediate(key, value); });
	}
}

MapReduce::Node* MapReduce::Merge(Node* a, Node* b)
{
	Node dummy;
    Node* tail = &dummy;
    dummy.next = nullptr;
    while (true)
    {
        if (a == nullptr)
        {
            tail->next = b;
            break;
        }
        else if (b == nullptr)
        {
            tail->next = a;
            break;
        }
        if (a->key <= b->key)
            MoveNode(&(tail->next), &a);
		else
            MoveNode(&(tail->next), &b);
 
        tail = tail->next;
    }
    return dummy.next;
}

void MapReduce::MoveNode(Node** destRef, Node** sourceRef)
{
	Node* newNode = *sourceRef;
	*sourceRef = newNode->next;

	newNode->next = *destRef;

	*destRef = newNode;
}

void MapReduce::Split(Node* src, Node** a, Node** b)
{
	if (src == nullptr || src->next == nullptr)
	{
		*a = src;
		*b = nullptr;
		return;
	}

	Node* slow = src;
	Node* fast = src->next;

	while (fast != nullptr)
	{
		fast = fast->next;
		if (fast != nullptr)
		{
			slow = slow->next;
			fast = fast->next;
		}

	}
	*a = src;
	*b = slow->next;
	slow->next = nullptr;
}

void MapReduce::MergeSort(Node** nodes)
{
	if (*nodes == nullptr || (*nodes)->next == nullptr)
		return;
	Node* a, *b;
	Split(*nodes, &a, &b);
	MergeSort(&a);
	MergeSort(&b);
	*nodes = Merge(a, b);
}

uint32_t MapReduce::Getter(const std::string& key, uint32_t partition)
{
	// printf("Getter");
	Node* cur = m_Partitions[partition].head;
	std::unique_lock<std::mutex> lock(m_Partitions[partition].mutex);
	if (m_IsNextKeyDifferent[partition] == 1)
	{
		m_IsNextKeyDifferent[partition] = 0;
		lock.unlock();
		return (uint32_t)-1;
	}

	if (cur != nullptr)
	{
		if (cur->key == key)
		{
			m_Partitions[partition].head = cur->next;
			if (cur->next != nullptr)
			{
				if (cur->next->key != key)
				{
					m_IsNextKeyDifferent[partition] = 1;
					lock.unlock();
					Backup(cur, partition);
					return cur->value;
				}
				lock.unlock();
				Backup(cur, partition);
				return cur->value;
			}
			else
			{
				lock.unlock();
				Backup(cur, partition);
				return cur->value;
			}
		}
	}
	return (uint32_t)-1;
}

uint64_t MapReduce::ParitionFunc(const std::string& key)
{
	return std::hash<std::string>()(key) % m_NumPartitions;
}

void MapReduce::EmitIntermediate(const std::string& key, uint32_t value)
{
	if (key.empty())
		return;
	uint64_t hash = ParitionFunc(key);
	Node* n = new Node();
	n->key = key;
	n->value = value;
	n->processed = false;
	
	{
		Lock lock(m_Partitions[hash].mutex);
		Node* it = m_Partitions[hash].head;
		if (it == nullptr)
		{
			m_Partitions[hash].head = n;
			n->next = nullptr;
			return;
		}
		n->next = m_Partitions[hash].head;
		m_Partitions[hash].head = n;
	}
}

void MapReduce::Emit(const std::string& key, uint32_t value, uint32_t partition)
{
	Lock lock(m_Partitions[partition].mutex);
	m_Partitions[partition].stream << key << " " << value << "\n";
}

void MapReduce::reduceFunction()
{	
	while (true)
	{
		uint32_t m_PartitionId  = 0;
		Node* it = nullptr;
		{
			Lock lock(m_PartitionMutex);
			if (m_NextPartition >= m_NumPartitions)
				return;
			
			it = m_Partitions[m_NextPartition].head;
			m_PartitionId = m_NextPartition;
			m_NextPartition++;
		}
		MergeSort(&m_Partitions[m_PartitionId].head);
		it = m_Partitions[m_PartitionId].head;
		while (it != nullptr)
		{
			m_MapTask->Reduce(it->key, [&](const std::string& key, uint32_t partition) { return Getter(key, partition); }, m_PartitionId, [&](const std::string& key, uint32_t value, uint32_t partition) { return Emit(key, value, partition); });
			it = m_Partitions[m_PartitionId].head;
		}
	}
}

void MapReduce::Backup(Node* cur, uint32_t partitionIdx)
{
	if (m_BackupPartitions[partitionIdx].head == nullptr) {
		m_BackupPartitions[partitionIdx].head = cur;
		cur->next = nullptr;
	}
	else {
		cur->next = m_BackupPartitions[partitionIdx].head;
		m_BackupPartitions[partitionIdx].head = cur;
	}
}

void MapReduce::Run(const Job& in)
{
	Job job = in;
	if (in.mapTask == nullptr)
		return;
	if (in.mapThreads == 0)
		job.mapThreads = std::thread::hardware_concurrency();
	if (in.reduceThreads == 0)
		job.reduceThreads = std::thread::hardware_concurrency();
	if (in.partitions == 0)
		job.partitions = std::thread::hardware_concurrency();

	m_NumPartitions = job.partitions;
	m_Partitions = new Partition[m_NumPartitions + 1];
	m_BackupPartitions = new Partition[m_NumPartitions + 1];
	m_MapTask = job.mapTask;
	m_IsNextKeyDifferent = new bool[m_NumPartitions];
	for (uint32_t i = 0; i < m_NumPartitions; i++)
	{
		m_IsNextKeyDifferent[i] = false;
		m_Partitions[i].stream = std::ofstream("part_" + std::to_string(i));
	}
	m_Files = job.filenames;
	m_InputFormat = job.inputFormat;

	for (const auto& filename : m_Files)
	{
		std::ifstream stream(filename);
		m_InputBuffer += std::string((std::istreambuf_iterator<char>(stream)), std::istreambuf_iterator<char>());
#ifdef CRLF
		m_InputBuffer += "\r\n";
#else
		m_InputBuffer += "\n";
#endif
	}

	// if (m_InputBuffer.size() < 1e6)
	if (m_InputFormat == InputFormat::Text)
	{
		uint32_t start = 0;
		for (uint32_t i = 0; i < m_InputBuffer.size(); i++)
		{
#ifdef CRLF
			if (m_InputBuffer[i] == '\r' && m_InputBuffer[i + 1] == '\n')
			{
				m_Lines.push_back({ start, i - 1 });
				start = i + 2;
				i++;
			}
#else
			if (m_InputBuffer[i] == '\n')
			{
				m_Lines.push_back({ start, i - 1 });
				start = i + 1;
			}
#endif
		}
	}
	
	uint32_t batchSize = m_Lines.size()  / job.mapThreads;
	uint32_t batchRemainder = m_Lines.size() % job.mapThreads;

	std::vector<std::thread> mapThreads;
	for (uint32_t i = 0; i < job.mapThreads; i++)
	{
		uint32_t start = i * batchSize;
		mapThreads.push_back(std::thread(&MapReduce::mapFunction, this, start, start + batchSize));
	}
	int start = job.mapThreads * batchSize;
	mapFunction(start, start + batchRemainder);

	for (uint32_t i = 0; i < mapThreads.size(); i++)
		mapThreads[i].join();

	std::vector<std::thread> reduceThreads;
	for (uint32_t i = 0; i < job.reduceThreads; i++)
		reduceThreads.push_back(std::thread(&MapReduce::reduceFunction, this));
	for (uint32_t i = 0; i < reduceThreads.size(); i++)
		reduceThreads[i].join();

	for (uint32_t i = 0; i < m_NumPartitions; i++)
	{
		m_Partitions[i].stream.close();
		Node* node = m_Partitions[i].head;
		while (node != nullptr)
		{
			Node* tmp = node;
			node = node->next;
			delete tmp;
		}
	}
	
	delete[] m_Partitions;
	delete[] m_BackupPartitions;
	delete[] m_IsNextKeyDifferent;
	
	if (job.mergeFiles)
	{
		fs::path outPath = job.outputFilename;
		if (job.mergeFiles && outPath.empty())
			outPath = "out.txt";
		std::ofstream output(outPath);

		for (uint32_t i = 0; i < m_NumPartitions; i++)
		{
			std::ifstream in("part_" + std::to_string(i));
			std::string res = std::string((std::istreambuf_iterator<char>(in)), std::istreambuf_iterator<char>());
			output << res;
		}
		output.close();
	}
}