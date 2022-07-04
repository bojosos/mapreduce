project "MapReduce"
	kind "StaticLib"
	language "C++"
	cppdialect "C++17"
	staticruntime "off"
	characterset ("MBCS")

	targetdir ("%{wks.location}/bin/" .. outputdir .. "/%{prj.name}")
	objdir ("%{wks.location}/bin-int/" .. outputdir .. "/%{prj.name}")

	files
	{
		"src/**.h",
		"src/**.cpp",
	}

	defines
	{
		"_CRT_NONSTDC_NO_DEPRECATE",
		"_CRT_SECURE_NO_WARNINGS"
	}

	includedirs
	{
		"src"
	}

    filter "system:windows"
		systemversion "latest"

	filter "configurations:Debug"
		runtime "Debug"
		symbols "on"

	filter "configurations:Release"
		runtime "Release"
		optimize "on"