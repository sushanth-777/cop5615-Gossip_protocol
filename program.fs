open System
open Akka.Actor
open Akka.FSharp
open System.Collections.Generic

// Initializing all message types that are sent and received by actors
type GossipMessageTypes =
    | Initailize of IActorRef []
    | InitializeVariables of int
    | CompletedMessage of String
    | PushSum of Double
    | ComputePushSum of Double * Double * Double
    | PushSumResult of Double * Double
    | Time of int
    | AllNodes of int
    | ActivateChildActor
    | CallChildActor

let mutable nodes = int (System.Environment.GetCommandLineArgs().[1]) // number of nodes
let topology = System.Environment.GetCommandLineArgs().[2] // Topology - gossip or push-sum
let protocol = System.Environment.GetCommandLineArgs().[3] // Protocol to use - line, 3D or Imperfect3D
let timer = Diagnostics.Stopwatch() // Timer to check convergence time of the algorithm
let system = ActorSystem.Create("System")

// We want a perfect cube for 3D and Imperfect 3D. Hence we take the nearest number of nodes which has a perfect cube root
let mutable actualNumOfNodes = nodes |> float
nodes <-
    match topology with
    | "Imp3D" -> 
        ((actualNumOfNodes ** 0.33334) |> floor ) ** 3.0 |> int
    | _ -> nodes

// An array of actors representing nodes in the topology
let mutable nodeArray = [||]

// Parent Actor Implementation
// Calculates convergence time. Activates the first actor. Has a counter to ensure all nodes received a message specified number of times.
let ParentActor(mailbox: Actor<_>) =
    
    let mutable count = 0
    let mutable start = 0
    let mutable AllNodes = 0

    let rec parentLoop() = actor{
        let! msg = mailbox.Receive()
        match msg with 
        | CompletedMessage _ -> 
            count <- count + 1
            if count = AllNodes then
                timer.Stop()
                printfn "-----------------------------------------------------------"
                printfn "Convergence Time: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | PushSumResult (sum, weight) ->
            count <- count + 1
            if count = AllNodes then
                timer.Stop()
                printfn "-----------------------------------------------------------"
                printfn "Convergence Time: %f ms" timer.Elapsed.TotalMilliseconds
                Environment.Exit(0)
        | Time strtTime -> start <- strtTime
        | AllNodes n -> AllNodes <- n
        | _ -> ()

        return! parentLoop()
    }            
    parentLoop()

// ParentActor
let parentActor = spawn system "ParentActor" ParentActor
let dictionary = new Dictionary<IActorRef, bool>()

// Child Actor Implementation
let ChildActor(mailbox: Actor<_>) =
    let mutable messageCount = 0
    let mutable neighbours: IActorRef [] = [||]
    let mutable sum = 0.0
    let mutable weight = 1.0
    let mutable termRound = 1
    let mutable alreadyConverged = false
    
    let rec childLoop() = actor{
        let! message = mailbox.Receive()
        
        match message with 
        | Initailize aref ->
            neighbours <- aref

        | ActivateChildActor ->
            // Find a random neighbor and send a message if the neighbor is active.
            let rnd = Random().Next(0, neighbours.Length)
            if not dictionary.[neighbours.[rnd]] then
                neighbours.[rnd] <! CallChildActor
            // Activate self.
            mailbox.Self <! ActivateChildActor

        | CallChildActor ->
            // Activate itself if the actor hasn't received any message
            if messageCount = 0 then 
                mailbox.Self <! ActivateChildActor
            // Inform parent when the actor received a message 10 times. Else, increase the counter.
            if (messageCount = 10) then 
                parentActor <! CompletedMessage "Message"
                dictionary.[mailbox.Self] <- true
            messageCount <- messageCount + 1
            
        | InitializeVariables number ->
            sum <- float number

        | PushSum delta ->
            // Choose a random neighbor. Calculate push-sum and send it to that neighbor 
            let index = Random().Next(0, neighbours.Length)

            sum <- sum / 2.0
            weight <- weight / 2.0
            neighbours.[index] <! ComputePushSum(sum, weight, delta)

        // Child actor checks if the ratio R=S/W changes no more than in C consecutive rounds (default: C=3) before sending any Push message.
        | ComputePushSum (s, w, delta) ->
            let newsum = sum + s
            let newweight = weight + w

            let cal = sum / weight - newsum / newweight |> abs

            if alreadyConverged then
                let index = Random().Next(0, neighbours.Length)
                neighbours.[index] <! ComputePushSum(s, w, delta)
            
            else
                if cal > delta then
                    termRound <- 0
                else 
                    termRound <- termRound + 1

                if  termRound = 3 then
                    termRound <- 0
                    alreadyConverged <- true
                    parentActor <! PushSumResult (sum, weight)

                sum <- newsum / 2.0
                weight <- newweight / 2.0
                let index = Random().Next(0, neighbours.Length)
                neighbours.[index] <! ComputePushSum(sum, weight, delta)
        | _ -> ()
        return! childLoop()
    }            
    childLoop()

// Build topologies
match topology with
| "line" ->
    nodeArray <- Array.zeroCreate (nodes + 1)
    // Spawn actors based on the number of nodes
    for x in [0..nodes] do
        let key: string = "Key" + string(x) 
        let actorRef = spawn system (key) ChildActor
        nodeArray.[x] <- actorRef 
        dictionary.Add(nodeArray.[x], false)
        nodeArray.[x] <! InitializeVariables x

    // Add the reference of neighbors for an actor in the neighbourArray
    for i in [ 0 .. nodes ] do
        let mutable neighbourArray = [||]
        if i = 0 then
            neighbourArray <- (Array.append neighbourArray [|nodeArray.[i+1]|])
        elif i = nodes then
            neighbourArray <- (Array.append neighbourArray [|nodeArray.[i-1]|])
        else 
            neighbourArray <- (Array.append neighbourArray [| nodeArray.[(i - 1)] ; nodeArray.[(i + 1 ) ] |] ) 
        
        nodeArray.[i] <! Initailize(neighbourArray)

    let leader = Random().Next(0, nodes)
   
    timer.Start()
    match protocol with
    | "gossip" -> 
        parentActor <! AllNodes(nodes)
        parentActor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Protocol Gossip"
        nodeArray.[leader] <! ActivateChildActor
        
    | "push-sum" -> 
        parentActor <! AllNodes(nodes)
        parentActor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Starting Push Sum Protocol for Line"
        nodeArray.[leader] <! PushSum(10.0 ** -10.0)     
    | _ ->
        printfn "Invalid:Please enter a proper protocol or topology"   

| "full" ->
    nodeArray <- Array.zeroCreate (nodes + 1)
    
    for x in [0..nodes] do
        let key: string = "demo" + string(x) 
        let actorRef = spawn system (key) ChildActor
        nodeArray.[x] <- actorRef 
        nodeArray.[x] <! InitializeVariables x
        dictionary.Add(nodeArray.[x], false)
    
    for i in [ 0 .. nodes ] do
        let mutable neighbourArray = [||]
        for j in [0..nodes] do 
            if i <> j then
                neighbourArray <- (Array.append neighbourArray [|nodeArray.[j]|])
        nodeArray.[i]<! Initailize(neighbourArray)
              

    timer.Start()
    // Choose a random childActor to start the gossip
    let leader = Random().Next(0, nodes)

    match protocol with
    | "gossip" -> 
        parentActor <! AllNodes(nodes)
        parentActor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Use Of Gossip Protocol"
        nodeArray.[leader] <! CallChildActor
    | "push-sum" -> 
        parentActor <! AllNodes(nodes)
        parentActor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Push Sum Started"
        nodeArray.[leader] <! PushSum(10.0 ** -10.0) 
    | _ ->
        printfn "Invalid:Please enter a proper protocol or topology"

| "2D" ->
    let gridSize = int (sqrt(float nodes) |> ceil)
    nodes <- gridSize * gridSize// rounding up to the nearest square


    
    nodeArray <- Array.zeroCreate (nodes + 1)

    for x in [0..nodes] do
        let key: string = "demo" + string(x) 
        let actorRef = spawn system (key) ChildActor
        nodeArray.[x] <- actorRef 
        nodeArray.[x] <! InitializeVariables x
        dictionary.Add(nodeArray.[x], false)
    
    for i in [ 0 .. nodes ] do
        let mutable neighbourArray = [||]
        if i > 0 then
            neighbourArray <- (Array.append neighbourArray [| nodeArray.[i - 1] |])
        if i < nodes then
            neighbourArray <- (Array.append neighbourArray [| nodeArray.[i + 1] |])
        nodeArray.[i]<! Initailize(neighbourArray)

    let leader = Random().Next(0, nodes)
    
    timer.Start()
    match protocol with
    | "gossip" -> 
        parentActor <! AllNodes(nodes)
        parentActor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Use Of Gossip Protocol"
        nodeArray.[leader] <! ActivateChildActor
    | "push-sum" -> 
        parentActor <! AllNodes(nodes)
        parentActor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Push Sum Started"
        nodeArray.[leader] <! PushSum(10.0 ** -10.0) 
    | _ ->
        printfn "Invalid: Please enter a proper protocol or topology"

| "Imp3D"  ->
    let _3DGridSize = (actualNumOfNodes ** 0.34)|>floor  |> int 
    nodeArray <- Array.zeroCreate (nodes+1)
    
    for x in [0..nodes] do
        let key: string = "demo" + string(x) 
        let actorRef = spawn system (key) ChildActor
   
        nodeArray.[x] <- actorRef 
        nodeArray.[x] <! InitializeVariables x
        dictionary.Add(nodeArray.[x], false)
        

    
    let zMulti =  _3DGridSize * _3DGridSize
    let yMulti = _3DGridSize
    let xLimit = _3DGridSize - 1
    let yLimit = _3DGridSize - 1
    let zLimit = _3DGridSize - 1

    for z in [ 0 .. (_3DGridSize-1)] do
        for y in [ 0 .. (_3DGridSize-1) ] do
            for x in [ 0 .. (_3DGridSize-1) ] do
                
                
                let i = z * zMulti + y * yMulti + x
                if i < nodes then
                    let mutable neighbours: IActorRef [] = [||]
                    if x > 0 then
                        neighbours <- (Array.append neighbours [| nodeArray.[i - 1] |])
                    if x < xLimit && (i + 1) < nodes then
                        neighbours <- (Array.append neighbours [| nodeArray.[i + 1] |])
                    if y > 0 then
                        neighbours <- (Array.append neighbours [| nodeArray.[i - yMulti] |])
                    if y < yLimit && (i + yMulti) < nodes then
                        neighbours <- (Array.append neighbours [| nodeArray.[i + yMulti] |])
                    if z > 0 then
                        neighbours <- (Array.append neighbours [| nodeArray.[i - zMulti] |])
                    if z < zLimit && (i + zMulti) < nodes then
                        neighbours <- (Array.append neighbours [| nodeArray.[i + zMulti] |])
                    match topology with 
                    | "Imp3D" -> 
                        let rnd = Random().Next(0, nodes-1)
                        neighbours <- (Array.append neighbours [|nodeArray.[rnd] |])
                        nodeArray.[i] <! Initailize(neighbours)
                    | _ -> 
                        printfn "Invalid:Please enter a proper topology"
   
    
    let leader = Random().Next(0, nodes)
    timer.Start()
    match protocol with 
    | "gossip" -> 
        parentActor <! AllNodes(nodes)
        parentActor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Use Of Gossip Protocol"
        nodeArray.[leader] <! ActivateChildActor
    | "push-sum" -> 
        parentActor <! AllNodes(nodes)
        parentActor <! Time(DateTime.Now.TimeOfDay.Milliseconds)
        printfn "Push Sum Started"
        nodeArray.[leader] <! PushSum(10.0 ** -10.0)
    | _ -> 
        printfn "Invalid:Please enter a proper protocol or topology"
| _ -> ()

// Ignore any other input from the user
Console.ReadLine() |> ignore