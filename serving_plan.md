I am on a tight deadline, 2 more days.

Your job is helping me plan and maybe creating agents. You dont code yourself. You help me plan and manage this project.

We have 3 locations, cluster with the core_v4 database 130GB. my laptop at home. And the vm seving the webapp, code is: /Users/wehrenberger/Code/DIGICHer/heritagemonitor.

VM and cluster cant be connected, so i have to probably compress, then download the data form the cluster to my mac, then send it to the VM, then we install hm_pipeline on the vm, create the indexes on there and drop the data directly in /home/lu72hip/Code/DIGICHer/heritagemonitor on the VMs opensearch. at least i think thats the easiest and fastest way, in case we have complications with the 
indexes we can always rebuild them, not sure how fast that ll go though, the vm has 32 GB ram, 4 cores i think and 5 TB disk. argue if u think there is a better way.

Oh for testing i will now create a small duckdb with 1000 of each entity and ensure there are actual relations so that we can test everything right away.

okay so much for the general process.

the actually hard part is now designing indexes in opensearch for /Users/wehrenberger/Code/DIGICHer/heritagemonitor given our core_v4_noworkenrichment.duckdb file.
I think these indexes have to be designed while keeping the actual UI and api needs in mind. 

i think the first step is designing the indexes top level, ie to see how they will be conntected with one another and how many there actually are. ie i guess i dont want to include all work titles and work abstracst in the project index. but i ll need the work ids's that are connected to a project in the project index, so when i look in the ui at a project, i can also fetch from the works collection a list of all works connected to the project.

in /Users/wehrenberger/Code/DIGICHer/heritagemonitor i already made 2 test collections that are small, one is the minority index only serving the raw minorities. one is a test index that i made for testing the collaborations in deck gl. both work good but are small. minority index supports facets and autocomplete.

so lets look at that, all in /Users/wehrenberger/Code/DIGICHer/heritagemonitor. we have 5 use cases, defined in /Users/wehrenberger/Code/DIGICHer/heritagemonitor/apps/web/src/common/catalog/useCases.ts

All use cases will use a very similar /search ui layout. minorities and default search already implements that. i have a general idea of each use case in my mind, but this needs to be fleshed out.

lets look at the search ui in general first

i have at the far top 0.) the search bar and entity selector. entities can only be selected on /search usecase. in the is center a 1.) paginated list with a button to 2.) rank the list (we dont do downloadable data). Next to that is a 3.) tabbedpannel that features per default an overview of the selected item. tabbedpannel also has tabs to include other stuff like another 4.) paginated list, in test minorities right now that lists the subgroups of the selected min. for projects that would list the linked projects and organisations. tabbedpannel would also house the deckgl map, probably as the first tab for the map use cases 5.) above these 2 are the filters. 6.) and on the left side are facets 7.) i also have a global corpus selector that switches between SCI and DCH. SCI is no filter, DCH would only list entities that are classified as is_ch (is_ch is actually means is digital cultural heritage a la DCH). so for projects thats direct, for works we dont have that enrichment, for minorities that would filter them based on their linked projects, same as for orgas. 8.) topics is an extra filter i have not included in heritagemonitor yet, but it exists in digicher_webinterface. it will open a modal on top of the screen that lists a tree of the topics (excluding the top level domain, so only 3 levels). topics modal has its own searchbar where users can search topics. its paramount that this topics modal also filters with the selected corpus, ie shows only the topics that actually have a project in DCH, when DCH is selected.

1st usecase) /search for  projects, works, orgas and grants search 
- projects: 
    -> the center piece. the paginated list would list all projects by ranking. default bm25, but also rankable by budget. no alphabetic ranking.
    -> on click the tabbedpanel would show an overview of all project information. 2nd tab would list its organisations, starting with the coordinator. 3d tab would list the related works. on click on an orga or work, should send the user to /search/works or /search/organisations via the id, show in the paginated list only the matching work or orga and their overview in the tabed panel. opinions on this?
    ->
    -> filters and facets, this is difficult. First should be year, then theme, then pillar, topics, grants
- works ...
    -> works are a lot for our rather weak vm and probably have to be shared. this is my first time using openesearch. 
- organisations (named organization in the duckdb, bust me be organisation and british in the webapp). 
- grants are where the funding comes from. grants could be messy given openaires data structure that drives core_v4. idk what the grants look like, we might have to let an agent analyse this on prod. we can do that for all open questions. i d like at least to have the actual EC funding programmes listed. there are also distinct funding codes which might be too many idk.

2nd usecase) /search/experts

- this is basically: search for projects and works, then list and rank the highest matching organisations.

3d usecase) /search/minorities
- this is the only use case that has to respect the minorities and it can be isolated from the other indexes as the base minority collection will be a lot smaller.
- we have a simple version of minorities currently and i like it, but we have new column names so we might need to change some dto's etc. i would like to include complete projects, works, grants and orgas directly to the minorities table. so that u can see all entities that involve a minority directly when u click on the minority. 
- searching for the minorities would give us the minorities, searching for text would give es minorities matching the works and projects for that minority. searching institution names would also show us the matching minorities
- i have deckgl installed and a small map that displays as icons the queries minorities given the organisations geolocation would be cool. deckgl is ready to be used. i d probably put the deckgl map in the tabbedpanel, not sure. but this would mean for the deckgl table we d need to run a 2nd query that requests all matching organisations and minority qids as well as the geolocation from the minority index. 

4th usecase) search/funding

- no idea how t make this useful
- basically projects and the funding they received and the grants maybe with orgas
- displaying on a deckgl map the organisations summed funding as columns. the region filter would be cool here
- btw deckgl is always 2nd. first and foremost the paginated list, the queries and facets have to matter. the map must derive from that, but also the map will need a 2nd request i fear, because well the map needs a tight full list (not paginated) of the ids and geolocations

5th usecase) collaboration -> actually 2 usecases

5.1) /search/collaboration/organisationNetwork collaboration network of an organisation. 
- that works very well in the old version of heritagemonitor: /Users/wehrenberger/Code/DIGICHer/digicher_webinterface/src/app/scenarios/collaboration
- basically u select an organisation by name, then u receive in the paginated list the organisations that collaborate with that found organisation by shared projects
- on the deckgl map the found organisation is centered and from it u have an arch to all its partner organisations
- the old digicher_webinterface made that complicated to find though. so it would be very imporatant that autocomplete for the organisations is supported. so user in the heropage selected " collaboration network of an organisation. " then the searchbar at the heropage autocompletes the users entered organisations, that autocomplete should also rank organisations by total number of projects. clicking on an organisation then send the user to directly to /search/collaboration/organisationNetwork where he sees the organisation network

5.2) /search/collaboration/partnerNetwork collaboration network of a query
- instead of showing arcs from 1 organisation, this should show a 3d-force collaboration network. i also tested this in digicher_webinterface


final notes

Features i want, when they are not too expensive
- autocomplete where it works, ie everywhere but works
- google search like search, ie with AND, direct matching "", and negation -
