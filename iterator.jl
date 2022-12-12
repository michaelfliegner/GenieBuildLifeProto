using JSON, LifeInsuranceDataModel, SearchLight, ToStruct

function compareRevisions(t, previous::Dict{String,Any}, current::Dict{String,Any})
    let changed = false
        for (key, previous_value) in previous
            if !(key in ("ref_validfrom", "ref_invalidfrom", "ref_component"))
                let current_value = current[key]
                    if previous_value != current_value
                        changed = true
                    end
                end
            end
        end
        if (changed)
            (ToStruct.tostruct(t, previous), ToStruct.tostruct(t, current))
        end
    end
end

x0 = [1, 2, 3, 4, 5]
y0 = [1, 2, 4, 5]

x = map(x0) do el
    ContractRevision(id=DbId(el))
end
y = map(y0) do el
    ContractRevision(id=(el !== 5 ? DbId(el) : DbId()), description=(el == 2 || el == 4 ? "previous" : ""), ref_invalidfrom=(el == 4 ? 14 : 9007199254740991))
end

previous = JSON.parse(JSON.json(x))
current = JSON.parse(JSON.json(y))
current_version = 14

changed = filter(x -> !isnothing(x), [compareRevisions(ContractRevision, a, b)
                                      for a in current for b in previous if a["id"] == b["id"] && (a["id"]["value"] !== nothing && a["ref_invalidfrom"]["value"] > current_version)])

new = [(nothing, el) for el in filter(el -> isnothing(el["id"]["value"]), current)]

deleted = [(nothing, el) for el in filter(el -> el["ref_invalidfrom"]["value"] == current_version, current)]

