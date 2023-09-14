local ServerStorage = game:GetService("ServerStorage")
local ReplicatedStorage = game:GetService("ReplicatedStorage")
local RunService = game:GetService("RunService")
local Players = game:GetService("Players")

local ProfileService = require(ServerStorage.DataService.ProfileService)
local Promise = require(ReplicatedStorage.Libraries.Promise)
local Signal = require(ReplicatedStorage.Modules.Signal)

local DATA_HEADER = "temp_Player_"

-- Based on the layout of the persistence library by BenSBk https://github.com/BenSBk/Persistence
-- Uses ProfileService and Promises

local profileStore, template
local playerProfiles = {}
local playerYields = {}
local profileReleased = setmetatable({}, {__mode = "k"})

local function deepCopy(base) -- clones a table so the original table isn't modified if anything is done to the copy
	local copy
	if typeof(base) == "table" then
		copy = {}
		for key, value in base do
			copy[deepCopy(key)] = deepCopy(value)
		end
	else
		copy = base
	end
	return copy
end

local function awaitProfile(player: Player)  -- based on Persistance's awaitData by BenSBk
	local profile = playerProfiles[player]
	if not profile then
		return Promise.new(function(resolve, reject)
			if profileReleased[player] ~= nil then
				reject(`{player.Name} userprofile wasn't acquired and was rejected by awaitProfile -> Promise, this is not an error and is intentional`)
			end

			local yields = if not playerYields[player] then {} else playerYields[player]
			playerYields[player] = yields

			local thread = coroutine.running() -- illegal promise method (trollres)
			table.insert(yields, thread)
			profile = coroutine.yield()

			if not profile then
				reject(`{player.Name} userprofile yield by awaitProfile -> Promise; player kicked beforehand, this is not an error and is intentional`)
			end

			resolve(profile.Data)
		end)
	end
	return Promise.resolve(profile.Data)
end

local function resumeYields(player, profile)
	local yields = playerYields[player]
	if not yields then return end
	playerYields[player] = nil

	for _, yield in ipairs(yields) do
		task.spawn(yield, profile)
	end
end


--[=[
	@class DataService
	
	A simple wrapper for ProfileService that allows for safe data editing at runtime
]=]
local DataService = {}

--[=[
	@server
	@param player Player
	@return Promise
]=]
function DataService.getAsync(player: Player)
	if not RunService:IsServer() then return end
	
	return awaitProfile(player)
end

--[=[
	@server
	@yields
	@param player Player
	@param key string -- specific index to return
	
	@return any
]=]
function DataService.awaitKey(player: Player, key: string)
	if not RunService:IsServer() then return end
	
	return DataService.getAsync(player):expect()[key]
end

--[=[
	@server
	@param player Player
	@param key string -- specific index to return
	@param set any -- replace value at index with set
	
	@return Promise
]=]
function DataService.setAsync(player: Player, key: string, set: any)  -- sets a key in a players data, waits until profile exists. Returns a promise
	if not RunService:IsServer() then return end
	
	return awaitProfile(player):andThen(function(data)
		data[key] = data[key] + set
		return data
	end):catch(warn)
end

--[=[
	@server
	@param player Player -- on player join
]=]
function DataService.acquire(player: Player) -- inits a specific profile
	if not RunService:IsServer() then return end
	if not profileStore then return end

	local profile = profileStore:LoadProfileAsync(`{DATA_HEADER}{tostring(player.UserId)}`)
	if not profile then
		player:Kick("The profile couldn't be loaded likely due to other Roblox servers (a certified Roblox moment), try rejoining the game")
	end

	profile:AddUserId(player.UserId)
	profile:Reconcile()
	profile:ListenToRelease(function()
		profileReleased[player] = true
		playerProfiles[player] = nil
		player:Kick()
		resumeYields(player, nil)
	end)
	if player:IsDescendantOf(Players) == true then
		playerProfiles[player] = profile
		resumeYields(player, profile)
	else
		profile:Release()
	end
end

--[=[
	@server
	@param player Player -- on player leave
]=]
function DataService.dispatch(player: Player) -- removes a specific profile
	if not RunService:IsServer() then return end
	
	local profile = playerProfiles[player]
	if profile then
		profile:Release()
	end
end

--[=[
	@tag Idempotent
	@server
	@param datastoreName string -- Name of datastore to initalize
	@param baseTemplate {[any]: any} -- table to load if no playerdata is found
]=]
function DataService.initKey(datastoreName: string, baseTemplate: {[any]: any})
	if not RunService:IsServer() then return end
	if profileStore then return end

	template = deepCopy(baseTemplate)
	profileStore = ProfileService.GetProfileStore(datastoreName, template)
end

return DataService