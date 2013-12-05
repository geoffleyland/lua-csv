pcall(require, "strict")
local csv = require"csv"

local errors = 0

local function test(filename, correct_result, parameters)
  local result = {}
  local f = csv.open(filename, parameters)
  for r in f:lines() do
    if not r[1] then
      local r2 = {}
      for k, v in pairs(r) do r2[#r2+1] = k..":"..tostring(v) end
      table.sort(r2)
      r = r2
    end
    result[#result+1] = table.concat(r, ",")
  end
  result = table.concat(result, "\n")
  if result ~= correct_result then
    io.stderr:write(
      ("Error reading '%s':\nExpected output:\n%s\n\nActual output:\n%s\n\n"):
      format(filename, correct_result, result))
    errors = errors + 1
  end
end


test("../test-data/embedded-newlines.csv", [[
embedded
newline,embedded
newline,embedded
newline
embedded
newline,embedded
newline,embedded
newline]])

test("../test-data/embedded-quotes.csv", [[
embedded "quotes",embedded "quotes",embedded "quotes"
embedded "quotes",embedded "quotes",embedded "quotes"]])

test("../test-data/header.csv", [[
alpha:ONE,bravo:two,charlie:3
alpha:four,bravo:five,charlie:6]], {header=true})

test("../test-data/header.csv", [[
apple:one,charlie:30
apple:four,charlie:60]],
{ columns = {
  apple = { name = "ALPHA", transform = string.lower },
  charlie = { transform = function(x) return tonumber(x) * 10 end }}})

test("../test-data/blank-line.csv", [[
this,file,ends,with,a,blank,line]])


if errors == 0 then
  io.stdout:write("Passed\n")
elseif errors == 1 then
  io.stdout:write("1 error\n")
else
  io.stdout:write(("%d errors\n"):format(errors))
end

os.exit(errors)