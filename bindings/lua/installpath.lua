-- Script to find the install path for a C module. Public domain.

if not arg or not arg[1] then
  io.write("Usage: lua installpath.lua modulename\n")
  os.exit(1)
end

local path = package.cpath

if arg[2] then
    path = package.path
end

for p in string.gmatch(path, "[^;]+") do
  if string.sub(p, 1, 1) ~= "." then
    local p2 = string.gsub(arg[1], "%.", string.sub(package.config, 1, 1))
    io.write(string.gsub(p, "%?", p2), "\n")
    return
  end
end
error("no suitable installation path found")
