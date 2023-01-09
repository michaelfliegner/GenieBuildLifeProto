module App

using GenieFramework
using BitemporalPostgres, DataStructures, JSON, LifeInsuranceDataModel, LifeInsuranceProduct, SearchLight, TimeZones, ToStruct
@genietools

CS_UNDO = Stack{Dict{String,Any}}()

@handlers begin
  @out activetxn::Bool = false
  @in command::String = ""
  @in prompt_new_txn::Bool = false
  @out contracts::Vector{Contract} = []
  @out contract_ids::Dict{Int64,Int64} = Dict{Int64,Int64}()
  @out current_contract::Contract = Contract()
  @in selected_contract_idx::Integer = -1
  @out current_workflow::Workflow = Workflow()
  @out partners::Vector{Partner} = []
  @out current_partner::Partner = Partner()
  @in selected_partner_idx::Integer = -1
  @out products::Vector{Product} = []
  @out current_product::Product = Product()
  @in selected_product_idx::Integer = -1
  @in new_contract_partner_role::Integer = 0
  @in new_contract_partner::Integer = 0
  @in selected_contractpartner_idx::Integer = -1
  @in selected_productitem_idx::Integer = -1
  @in selected_version::String = ""
  @out current_version::Integer = 0
  @out txn_time::ZonedDateTime = now(tz"UTC")
  @in newreftime::String = ""
  @out ref_time::ZonedDateTime = now(tz"UTC")
  @out histo::Vector{Dict{String,Any}} = Dict{String,Any}[]
  @in cs::Dict{String,Any} = Dict{String,Any}("loaded" => "false")
  @out cs_persisted = Dict{String,Any}()
  @out ps::Dict{String,Any} = Dict{String,Any}("loaded" => "false")
  @out prs::Dict{String,Any} = Dict{String,Any}("loaded" => "false")
  @in selected_product_part_idx::Integer = 0
  @in tab::String = "csection"
  @in leftDrawerOpen::Bool = false
  @in show_contract_partners::Bool = false
  @in show_product_items::Bool = false
  @in selected_product::Integer = 0
  @in show_tariff_item_partners::Bool = false
  @in show_tariff_items::Bool = false
  @out rolesContractPartner::Vector{Dict{String,Any}} = []
  @out rolesTariffItem::Vector{Dict{String,Any}} = []
  @out rolesTariffItemPartner::Vector{Dict{String,Any}} = []

  @onchange isready begin
    LifeInsuranceDataModel.connect()
    rolesContractPartner = load_role(LifeInsuranceDataModel.ContractPartnerRole)
    rolesTariffItem = load_role(LifeInsuranceDataModel.TariffItemRole)
    rolesTariffItemPartner = load_role(LifeInsuranceDataModel.TariffItemPartnerRole)
    @show rolesContractPartner
    @show rolesTariffItem
    @show rolesTariffItemPartner
    cs["loaded"] = "false"
    prs = Dict{String,Any}("loaded" => "false")
    ps = Dict{String,Any}("loaded" => "false")
    @show "App is loaded"
    tab = "contracts"
  end
  @onchange prompt_new_txn begin
    @info "prompt_new_txn pressed"
    @show prompt_new_txn
    if !prompt_new_txn
      @show newreftime
      @show ref_time
      if newreftime == ""
        @info "cancelled"
      else
        @info "should parse now"
        n = replace(newreftime, "/" => "-")
        @show n
        ref_time = ZonedDateTime(DateTime(n), tz"UTC")
        w1 = Workflow(
          type_of_entity="Contract",
          tsw_validfrom=ref_time,
          ref_history=current_contract.ref_history
        )
        update_entity!(w1)
        activetxn = true
        cs = JSON.parse(JSON.json(LifeInsuranceDataModel.csection(current_contract.id.value, now(tz"UTC"), ref_time, activetxn ? 1 : 0)))
        cs["loaded"] = "true"
        cs_persisted = deepcopy(cs)

        @info "cs==cs_persisted?"
        @show cs == cs_persisted
        @show cs
        @show cs_persisted
        push!(CS_UNDO, cs_persisted)
      end
    else
      @info "öffnen"
    end
  end

  @onchange selected_contract_idx begin
    if (selected_contract_idx >= 0)
      @show "selected_contract_idx"
      @show selected_contract_idx
      @info "enter selected_contract_idx"
      try
        current_contract = contracts[selected_contract_idx+1]
        activetxn = length(find(ValidityInterval, SQLWhereExpression("ref_history=? and is_committed=0", current_contract.ref_history))) == 1

        @show current_contract
        @show activetxn

        if activetxn
          current_workflow = find(Workflow, SQLWhereExpression("ref_history=? and is_committed=0", current_contract.ref_history))[1]
          ref_time = current_workflow.tsw_validfrom
          histo = map(convert, LifeInsuranceDataModel.history_forest(current_contract.ref_history.value).shadowed)
          cs = JSON.parse(JSON.json(LifeInsuranceDataModel.csection(current_contract.id.value, now(tz"UTC"), ref_time, activetxn ? 1 : 0)))
          cs["loaded"] = "true"
        else
          ref_time = now(tz"UTC")
          histo = map(convert, LifeInsuranceDataModel.history_forest(current_contract.ref_history.value).shadowed)
          cs = JSON.parse(JSON.json(LifeInsuranceDataModel.csection(current_contract.id.value, now(tz"UTC"), now(tz"UTC"), activetxn ? 1 : 0)))
          cs["loaded"] = "true"
        end
        @show current_workflow
        cs_persisted = deepcopy(cs)
        @info "cs==cs_persisted?"
        @show cs == cs_persisted
        @show cs
        @show cs_persisted
        push!(CS_UNDO, cs_persisted)

        if cs["product_items"] != []
          ti = cs["product_items"][1]["tariff_items"][1]
          tistruct = ToStruct.tostruct(LifeInsuranceDataModel.TariffItemSection, ti)
          LifeInsuranceProduct.calculate!(tistruct)
          cs["product_items"][1]["tariff_items"][1] = JSON.parse(JSON.json(tistruct))
          push!(__model__)
          @info("calculated")
          @show cs["loaded"]
          @info (cs["product_items"][1]["tariff_items"][1]["tariff_ref"]["rev"]["annuity_immediate"])
        end
        tab = "csection"
        selected_contract_idx = -1
        @info "contract loaded"
        @show cs_persisted
      catch err
        println("wassis shief gegangen ")
        @error "ERROR: " exception = (err, catch_backtrace())
      end
    end
  end

  @onchange selected_partner_idx begin
    @show selected_partner_idx
    @info "selected_partner_idx"
    if (selected_partner_idx >= 0)
      @show selected_partner_idx
      @info "enter selected_partner_idx"
      try
        current_partner = partners[selected_partner_idx+1]
        # histo = map(convert, LifeInsuranceDataModel.history_forest(current_contract.ref_history.value).shadowed)
        ps = JSON.parse(JSON.json(LifeInsuranceDataModel.psection(current_partner.id.value, now(tz"UTC"), now(tz"UTC"), activetxn ? 1 : 0)))
        ps["loaded"] = "true"
        selected_partner_idx = -1
        ps["loaded"] = "true"
        @show ps["loaded"]
        tab = "partner"
        @show tab
      catch err
        println("wassis shief gegangen ")
        @error "ERROR: " exception = (err, catch_backtrace())
      end
    end
  end
  @onchange selected_product_idx begin
    @show selected_product_idx
    @info "selected_product_idx"
    if (selected_product_idx >= 0)
      @show selected_product_idx
      @info "enter selected_product_idx"
      try
        current_product = products[selected_product_idx+1]
        # histo = map(convert, LifeInsuranceDataModel.history_forest(current_contract.ref_history.value).shadowed)
        prs = JSON.parse(JSON.json(LifeInsuranceDataModel.prsection(current_product.id.value, now(tz"UTC"), now(tz"UTC"), activetxn ? 1 : 0)))
        selected_product_idx = -1
        prs["loaded"] = "true"
        @show prs["loaded"]
        tab = "product"
        @show tab
      catch err
        println("wassis shief gegangen ")
        @error "ERROR: " exception = (err, catch_backtrace())
      end
    end
  end


  @onchange selected_contractpartner_idx begin
    if selected_contractpartner_idx != -1
      @show selected_contractpartner_idx
    end
  end

  @onchange selected_productitem_idx begin
    if selected_productitem_idx != -1
      @show selected_productitem_idx
      selected_productitem_idx = -1
    end
  end

  @onchange cs begin
    @info "contract structure modified"
    @show cs
  end

  @onchange command begin
    try
      @show command

      if command == "add productitem"
        @show command
        command = ""
      end
      if command == "add contractpartner"
        @show command
        @show cs["partner_refs"]


        cprj = JSON.parse(JSON.json(ContractPartnerReference(
          rev=ContractPartnerRefRevision(ref_role=DbId(new_contract_partner_role), ref_partner=DbId(new_contract_partner)),
          ref=PartnerSection())))
        new_contract_partner_role = 0
        new_contract_partner = 0
        append!(cs["partner_refs"], [cprj])
        @show cs["partner_refs"]
        @info "anzahl prefs= "
        @info length(cs["partner_refs"])
        push!(__model__)
        command = ""
      end
      if command == "create contract"
        activetxn = true
        w1 = Workflow(
          type_of_entity="Contract",
          tsw_validfrom=ref_time,
        )
        create_entity!(w1)
        c = Contract()
        cr = ContractRevision(description="contract creation properties")
        create_component!(c, cr, w1)
        current_workflow = w1
        current_contract = c
        @show command
        command = ""
        tab = ""
        tab = "contracts"
      end

      if command == "start transaction"
        activetxn = true
        w1 = Workflow(
          type_of_entity="Contract",
          ref_history=current_contract.ref_history,
          tsw_validfrom=ref_time,
        )
        update_entity!(w1)
        current_workflow = w1
        cs = JSON.parse(JSON.json(LifeInsuranceDataModel.csection(current_contract.id.value, now(tz"UTC"), ref_time, activetxn ? 1 : 0)))
        cs["loaded"] = "true"
        push!(__model__)
        @show command
        command = ""

      end

      #if isnothing(cs["partner_refs"][idx+1]["rev"]["id"]["value"])
      #  deleteat!(cs["partner_refs"], idx + 1)
      #  @info "after delete new cp"
      #else
      #  cs["partner_refs"][idx+1]["rev"]["ref_invalidfrom"]["value"] = current_workflow.ref_version
      #  @info "after delete persisted cp"
      #end


      if startswith(command, "delete_contract_partner")

        @show command


        @show first(CS_UNDO)["partner_refs"]
        idx = parse(Int64, chopprefix(command, "delete_contract_partner:"))

        @show cs["partner_refs"][idx+1]["rev"]
        if isnothing(cs["partner_refs"][idx+1]["rev"]["id"]["value"])
          deleteat!(cs["partner_refs"], idx + 1)
          @info "after delete new cp"
        else
          cs["partner_refs"][idx+1]["rev"]["ref_invalidfrom"]["value"] = current_workflow.ref_version
          @show cs["partner_refs"][idx+1]["rev"]
          @info "after delete persisted cp"
        end
        push!(__model__)
      end

      if command == "pop"
        @info "before pop"
        @show first(CS_UNDO)["partner_refs"]
        cs = pop!(CS_UNDO)
        push!(__model__)
        @info "after pop"
        @show cs["partner_refs"]
      end

      if command == "push"
        push!(CS_UNDO, deepcopy(cs))
        @show first(CS_UNDO)["partner_refs"]
      end

      if command == "persist"
        @show command
        @show cs_persisted
        deltas = compareModelStateContract(cs_persisted, cs, current_workflow)
        @info "showing deltas"
        @show deltas
        @info "ende deltas"
        for delta in deltas
          prev = delta[1]
          curr = delta[2]
          if !isnothing(prev) # component is not new, db identity has been set 
            @info "preexisting component"
            @show curr.ref_invalidfrom.value
            @show prev.ref_invalidfrom.value
            @show current_workflow.ref_version.value
            @show curr.ref_invalidfrom.value == current_workflow.ref_version.value
            if parse(Int, curr.ref_invalidfrom.value) == current_workflow.ref_version.value# component has just been deleted 
              @info "deleting component"
              @show curr
              delete_component!(curr, current_workflow)
            else
              @info "comparing component"
              update_component!(prev, curr, current_workflow)
            end
          else
            @info("new component ")
            @show curr
            @info "Type is" * string(typeof(curr))
            ct = get_typeof_component(curr)
            @show ct
            @info "Component Type is" * string(ct)
            # ContractPartnerRef
            # Workflow
            @show current_workflow.ref_history
            @show current_workflow.ref_version
            @show current_contract.id
            currc = ct(ref_history=current_workflow.ref_history, ref_version=current_workflow.ref_version,
              ref_super=current_contract.id)
            @show currc
            create_component!(currc, curr, current_workflow)

          end
        end
        cs = JSON.parse(JSON.json(LifeInsuranceDataModel.csection(current_contract.id.value, txn_time, ref_time, activetxn ? 1 : 0)))
        cs["loaded"] = "true"
        push!(__model__)
        command = ""
      end
      if command == "commit"
        @show command
        @show current_workflow
        commit_workflow!(current_workflow)
        activetxn = 0
        current_workflow = Workflow()
        command = ""
      end
      if command == "rollback"
        @show command
        @show current_workflow
        rollback_workflow!(current_workflow)
        activetxn = 0
        current_workflow = Workflow()
        command = ""
      end

    catch err
      println("wassis shief gegangen ")

      @error "ERROR: " exception = (err, catch_backtrace())
    end
  end

  @onchange selected_version begin
    @info "version handler"
    @show selected_version
    if selected_version != ""
      @show tab
      try
        node = fn(histo, selected_version)
        @info "node"
        @show node
        activetxn = (node["interval"]["is_committed"] == 0 ? true : false)
        txn_time = node["interval"]["tsdb_validfrom"]
        ref_time = node["interval"]["tsworld_validfrom"]
        current_version = parse(Int, selected_version)
        @show activetxn
        @show txn_time
        @show ref_time
        @show current_version
        @info "vor csection"
        cs = JSON.parse(JSON.json(LifeInsuranceDataModel.csection(current_contract.id.value, txn_time, ref_time, activetxn ? 1 : 0)))
        cs["loaded"] = "true"
        @info "vor tab "
        tab = "csection"
        ti = LifeInsuranceProduct.calculate!(cs["product_items"][1].tariff_items[1])
        print("ti=")
        println(ti)
      catch err
        println("wassis shief gegangen ")

        @error "ERROR: " exception = (err, catch_backtrace())
      end
    end
  end

  @onchange tab begin

    @show tab

    if tab == "contracts"
      current_contract = Contract()
      contracts = LifeInsuranceDataModel.get_contracts()
      let df = SearchLight.query("select distinct c.id, w.is_committed from contracts c join histories h on c.ref_history = h.id join workflows w on w.ref_history = h.id ")
        contract_ids = Dict(Pair.(df.id, df.is_committed))
      end
      @show contract_ids
      @info "contractsModel pushed"
    end

    if (tab == "partners")
      partners = LifeInsuranceDataModel.get_partners()
      @info "read partners"
    end
    if (tab == "products")
      products = LifeInsuranceDataModel.get_products()
      @info "read products"
    end
    if (tab == "csection")
      @show tab
    end
    if (tab == "product")
      @show tab
    end
    if (tab == "partner")
      @show tab
    end
    @info "leave tab handler"
  end
end

"""
convert(node::BitemporalPostgres.Node)::Dict{String,Any}

provides the view for the history forest from tree data the contracts/partnersModel delivers
"""
function convert(node::BitemporalPostgres.Node)::Dict{String,Any}
  i = Dict(string(fn) => getfield(getfield(node, :interval), fn) for fn in fieldnames(ValidityInterval))
  shdw = length(node.shadowed) == 0 ? [] : map(node.shadowed) do child
    convert(child)
  end
  Dict("version" => string(i["ref_version"]), "interval" => i, "children" => shdw, "label" => "committed " * string(i["tsdb_validfrom"]) * " valid as of " * string(Date(i["tsworld_validfrom"], UTC)))
end


"""
fn
retrieves a history node from its label 
"""

function fn(ns::Vector{Dict{String,Any}}, lbl::String)
  for n in ns
    if (n["version"] == lbl)
      return (n)
    else
      if (length(n["children"]) > 0)
        m = fn(n["children"], lbl)
        if !isnothing((typeof(m)))
          return m
        end
      end
    end
  end
end
"""
compareRevisions(t, previous::Dict{String,Any}, current::Dict{String,Any}) where {T<:BitemporalPostgres.ComponentRevision}
compare corresponding revision elements and return nothing if equal a pair of both else
"""
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

"""
compareModelStateContract(previous::Dict{String,Any}, current::Dict{String,Any}, w::Workflow)
	compare viewmodel state for a contract section
"""
function compareModelStateContract(previous::Dict{String,Any}, current::Dict{String,Any}, w::Workflow)
  diff = []
  @show current["revision"]
  @show previous
  cr = compareRevisions(ContractRevision, previous["revision"], current["revision"])
  if (!isnothing(cr))
    push!(diff, cr)
  end
  @info "comparing Partner_refs"
  for i in 1:length(current["partner_refs"])
    @show current["partner_refs"]
    curr = current["partner_refs"][i]["rev"]
    @info "current pref rev"
    @show curr
    if isnothing(curr["id"]["value"])
      @info ("INSERT" * string(i))
      push!(diff, (nothing, ToStruct.tostruct(ContractPartnerRefRevision, curr)))
    else
      prev = previous["partner_refs"][i]["rev"]
      if curr["ref_invalidfrom"]["value"] == w.ref_version
        @info ("DELETE" * string(i))
        push!(diff, (ToStruct.tostruct(ContractPartnerRefRevision, prev), ToStruct.tostruct(ContractPartnerRefRevision, curr)))
        @info "DIFF="
        @show diff
      else
        @info ("UPDATE" * string(i))
        cprr = compareRevisions(ContractPartnerRefRevision, prev, curr)
        if (!isnothing(cprr))
          push!(diff, cprr)
        end
      end
    end
  end
  @info "final DIFF"
  @show diff
  diff
end

function load_role(role)
  LifeInsuranceDataModel.connect()
  map(find(role)) do entry
    Dict{String,Any}("value" => entry.id.value, "label" => entry.value)
  end
end


@page("/", "app.jl.html")

end