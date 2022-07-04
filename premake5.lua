workspace "MapReduce"
	architecture "x86_64"
	startproject "CLI"

	configurations
	{
		"Debug",
		"Release",
		"Dist"
	}

	platforms
	{
		"Win64",
	}
	
	flags
	{
		"MultiProcessorCompile"
	}

outputdir = "%{cfg.buildcfg}-%{cfg.system}-%{cfg.architecture}"

include "CLI"
include "Sandbox"
include "MapReduce"