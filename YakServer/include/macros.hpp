#ifndef MACROS_HPP
#define MACROS_HPP
//Source: http://gcc.gnu.org/wiki/Visibility
#if defined _WIN32 || defined __CYGWIN__
#ifdef BUILDING_DLL
#ifdef __GNUC__
#define PUBLIC __attribute__ ((dllexport))
#else
#define PUBLIC __declspec(dllexport) // Note: actually gcc seems to also supports this syntax.
#endif
#else
#ifdef __GNUC__
#define PUBLIC __attribute__ ((dllimport))
#else
#define PUBLIC __declspec(dllimport) // Note: actually gcc seems to also supports this syntax.
#endif
#endif
#define HIDDEN
#else
#if __GNUC__ >= 4
#define PUBLIC __attribute__((visibility ("default")))
#define HIDDEN  __attribute__((visibility ("hidden")))
#define INTERNAL __attribute__((visibility ("internal")))
//Optimization hints
#define HOT __attribute__((hot))
#define COLD __attribute__((cold))
//Branch prediction hints
#define likely(x) __builtin_expect((x), 1)
#define unlikely(x) __builtin_expect((x), 0)
//Others
#define PACKED __attribute__((packed))
#else
#warn No visibility attributes! 
#define PUBLIC
#define HIDDEN
#define INTERNAL
#define HOT
#define COLD
#define likely(x) (x)
#define unlikely(x) (x)
#define PACKED
#endif

#endif

#endif  /* MACROS_HPP */
