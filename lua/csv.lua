--- Read a comma or tab (or other delimiter) separated file.
--  This version of a CSV reader differs from others I've seen in that it
--
--  + handles embedded newlines in fields (if they're delimited with double
--    quotes)
--  + is line-ending agnostic
--  + reads the file line-by-line, so it can potientially handle large
--    files.
--
--  Of course, for such a simple format, CSV is horribly complicated, so it
--  likely gets something wrong.

--  (c) Copyright 2013-2014 Incremental IP Limited.
--  (c) Copyright 2014 Kevin Martin
--  Available under the MIT licence.  See LICENSE for more information.

local DEFAULT_BUFFER_BLOCK_SIZE = 1024 * 1024


------------------------------------------------------------------------------

local function trim_space(s)
  return s:match("^%s*(.-)%s*$")
end


local function fix_quotes(s)
  -- the sub(..., -2) is to strip the trailing quote
  return string.sub(s:gsub('""', '"'), 1, -2)
end


------------------------------------------------------------------------------

--- Parse a list of columns.
--  The main job here is normalising column names and dealing with columns
--  for which we have more than one possible name in the header.
local function build_column_name_map(columns)
  local column_name_map = {}
  for n, v in pairs(columns) do
    local names
    local t
    if type(v) == "table" then
      t = { transform = v.transform, default = v.default }
      if v.name then
        names = { (v.name:gsub("_+", " ")) }
      elseif v.names then
        names = v.names
        for i, n in ipairs(names) do names[i] = n:gsub("_+", " ") end
      end
    else
      if type(v) == "function" then
        t = { transform = v }
      else
        t = {}
      end
    end

    if not names then
      names = { (n:lower():gsub("_", " ")) }
    end

    t.name = n
    for _, n in ipairs(names) do
      column_name_map[n:lower()] = t
    end
  end

  return column_name_map
end


--- Map "virtual" columns to file columns.
--  Once we've read the header, work out which columns we're interested in and
--  what to do with them.  Mostly this is about checking we've got the columns
--  we need and writing a nice complaint if we haven't.
local function build_column_index_map(header, column_name_map)
  local column_index_map = {}

  -- Match the columns in the file to the columns in the name map
  local found = {}
  for i, word in ipairs(header) do
    word = word:lower():gsub("[^%w%d]+", " "):gsub("^ *(.-) *$", "%1")
    local r = column_name_map[word]
    if r then
      column_index_map[i] = r
      found[r.name] = true
    end
  end

  -- check we found all the columns we need
  local not_found = {}
  for name, r in pairs(column_name_map) do
    if not found[r.name] then
      local nf = not_found[r.name]
      if nf then
        nf[#nf+1] = name
      else
        not_found[r.name] = { name }
      end
    end
  end
  -- If any columns are missing, assemble an error message
  if next(not_found) then
    local problems = {}
    for k, v in pairs(not_found) do
      local missing
      if #v == 1 then
        missing = "'"..v[1].."'"
      else
        missing = v[1]
        for i = 2, #v - 1 do
          missing = missing..", '"..v[i].."'"
        end
        missing = missing.." or '"..v[#v].."'"
      end
      problems[#problems+1] = "Couldn't find a column named "..missing
    end
    error(table.concat(problems, "\n"), 0)
  end

  return column_index_map
end


local function transform_field(value, index, map, filename, line, column)
  local field = map[index]
  if field then
    if field.transform then
      local ok
      ok, value = pcall(field.transform, value)
      if not ok then
        error(("%s:%d:%d: Couldn't read field '%s': %s"):
              format(filename or "<unknown>", line, column,
              field.name, value))
      end
    end
    return value or field.default, field.name
  end
end


------------------------------------------------------------------------------

local file_buffer = {}
file_buffer.__index = file_buffer

function file_buffer:new(file, buffer_block_size)
  return setmetatable({
      file              = file,
      buffer_block_size = buffer_block_size or DEFAULT_BUFFER_BLOCK_SIZE,
      buffer_start      = 0,
      buffer            = "",
    }, file_buffer)
end


--- Cut the front off the buffer if we've already read it
function file_buffer:truncate(p)
  p = p - self.buffer_start
  if p > self.buffer_block_size then
    local remove = self.buffer_block_size *
      math.floor((p-1) / self.buffer_block_size)
    self.buffer = self.buffer:sub(remove + 1)
    self.buffer_start = self.buffer_start + remove
  end
end


--- Find something in the buffer, extending it if necessary
function file_buffer:find(pattern, init)
  while true do
    local first, last, capture =
      self.buffer:find(pattern, init - self.buffer_start)
    -- if we found nothing, or the last character is at the end of the
    -- buffer (and the match could potentially be longer) then read some
    -- more.
    if not first or last == #self.buffer then
      local s = self.file:read(self.buffer_block_size)
      if not s then
        if not first then
          return
        else
          return first + self.buffer_start, last + self.buffer_start, capture
        end
      end
      self.buffer = self.buffer..s
    else
      return first + self.buffer_start, last + self.buffer_start, capture
    end
  end
end


--- Extend the buffer so we can see more
function file_buffer:extend(offset)
  local extra = offset - #self.buffer - self.buffer_start
  if extra > 0 then
    local size = self.buffer_block_size *
      math.ceil(extra / self.buffer_block_size)
    local s = self.file:read(size)
    if not s then return end
    self.buffer = self.buffer..s
  end
end


--- Get a substring from the buffer, extending it if necessary
function file_buffer:sub(a, b)
  self:extend(b)
  b = b == -1 and b or b - self.buffer_start
  return self.buffer:sub(a - self.buffer_start, b)
end


--- Close a file buffer
function file_buffer:close()
  self.file:close()
  self.file = nil
end


------------------------------------------------------------------------------

--- Iterate through the records in a file
--  Since records might be more than one line (if there's a newline in quotes)
--  and line-endings might not be native, we read the file in chunks of
--  we read the file in chunks using a file_buffer, rather than line-by-line
--  using io.lines.
local function separated_values_iterator(buffer, parameters)
  local filename = parameters.filename or "<unknown>"
  local field_start = 1
  local line_start = 1
  local line = 1
  local column_name_map = parameters.columns and
    build_column_name_map(parameters.columns)
  local column_index_map

  local advance
  if buffer.truncate then
    advance = function(n)
      field_start = field_start + n
      buffer:truncate(field_start)
    end
  else
    advance = function(n)
      field_start = field_start + n
    end
  end


  local function field_sub(a, b)
    b = b == -1 and b or b + field_start - 1
    return buffer:sub(a + field_start - 1, b)
  end


  local function field_find(pattern, init)
    local f, l, c = buffer:find(pattern, init + field_start - 1)
    if not f then return end
    return f - field_start + 1, l - field_start + 1, c
  end


  -- If the user hasn't specified a separator, try to work out what it is.
  local sep = parameters.separator
  if not sep then
    local _
    _, _, sep = buffer:find("([,\t])", 1)
  end
  sep = "(["..sep.."\n\r])"


  -- Start reading the file
  local field_count, fields, starts = 0, {}, {}
  local header

  while true do
    local field_start_line = line
    local field_start_column = field_start - line_start + 1
    local field_end, sep_end, this_sep
    local tidy

    -- If the field is quoted, go find the other quote
    if field_sub(1, 1) == '"' then
      advance(1)
      local current_pos = 0
      repeat
        local a, b, c = field_find('"("?)', current_pos + 1)
        current_pos = b
      until c ~= '"'
      if not current_pos then
        error(("%s:%d:%d: unmatched quote"):
          format(filename, field_start_line, field_start_column))
      end
      tidy = fix_quotes
      field_end, sep_end, this_sep = field_find(" *([^ ])", current_pos+1)
      if this_sep and not this_sep:match(sep) then
        error(("%s:%d:%d: unmatched quote"):
          format(filename, field_start_line, field_start_column))
      end
    else
      field_end, sep_end, this_sep = field_find(sep, 1)
      tidy = trim_space
    end

    -- Look for the separator or a newline or the end of the file
    field_end = (field_end or 0) - 1

    -- Read the field, then convert all the line endings to \n, and
    -- count any embedded line endings
    local value = field_sub(1, field_end)
    value = value:gsub("\r\n", "\n"):gsub("\r", "\n")
    for nl in value:gmatch("\n()") do
      line = line + 1
      line_start = nl + field_start
    end

    value = tidy(value)
    field_count = field_count + 1

    -- Insert the value into the table for this "line"
    local key
    if column_index_map then
      value, key = transform_field(value, field_count, column_index_map,
        filename, field_start_line, field_start_column)
    elseif header then
      key = header[field_count]
    else
      key = field_count
    end
    if key then
      fields[key] = value
      starts[key] = { line=field_start_line, column=field_start_column }
    end

    -- if we ended on a newline then yield the fields on this line.
    if not this_sep or this_sep == "\r" or this_sep == "\n" then
      if column_name_map and not column_index_map then
        column_index_map = build_column_index_map(fields, column_name_map)
      elseif parameters.header and not header then
        header = fields
      else
        local k, v = next(fields)
        if v ~= "" or field_count > 1 then  -- ignore blank lines
          coroutine.yield(fields, starts)
        end
      end
      field_count, fields, starts = 0, {}, {}
    end

    -- If we *really* didn't find a separator then we're done.
    if not sep_end then break end

    -- If we ended on a newline then count it.
    if this_sep == "\r" or this_sep == "\n" then
      if this_sep == "\r" and field_sub(sep_end+1, sep_end+1) == "\n" then
        sep_end = sep_end + 1
      end
      line = line + 1
      line_start = field_start + sep_end
    end

    advance(sep_end)
  end
end


------------------------------------------------------------------------------

local buffer_mt =
{
  lines = function(t)
      return coroutine.wrap(function()
          separated_values_iterator(t.buffer, t.parameters)
        end)
    end,
  close = function(t)
      if t.buffer.close then t.buffer:close() end
    end,
  name = function(t)
      return t.parameters.filename
    end,
}
buffer_mt.__index = buffer_mt


local function use(buffer, parameters)
  local f = { buffer = buffer, parameters = parameters }
  return setmetatable(f, buffer_mt)
end


------------------------------------------------------------------------------

--- Open a file for reading as a delimited file
--  @return a file object
local function open(
  filename,         -- string: name of the file to open
  parameters)       -- ?table: parameters controlling reading the file.
                    -- See README.md
  local file, message = io.open(filename, "r")
  if not file then return nil, message end

  parameters = parameters or {}
  parameters.filename = filename
  return use(file_buffer:new(file), parameters)
end


------------------------------------------------------------------------------

local function makename(s)
  local t = {}
  t[#t+1] = "<(String) "
  t[#t+1] = (s:gmatch("[^\n]+")() or ""):sub(1,15)
  if #t[#t] > 14 then t[#t+1] = "..." end
  t[#t+1] = " >"
  return table.concat(t)
end


--- Open a string for reading as a delimited file
--  @return a file object
local function openstring(
  filecontents,     -- string: The contents of the delimited file
  parameters)       -- ?table: parameters controlling reading the file.
                    -- See README.md

  parameters = parameters or {}


  parameters.filename = parameters.filename or makename(s)
  parameters.buffer_size = parameters.buffer_size or #filecontents
  return use(filecontents, parameters)
end


------------------------------------------------------------------------------

return { open = open, openstring = openstring, use = use }

------------------------------------------------------------------------------
