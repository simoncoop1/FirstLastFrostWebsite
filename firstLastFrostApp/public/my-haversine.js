//c1 is [longitude1,latitude1]
//c2 is [longitude2,latitude2]
//the function works out distance(meters) between
function myHaversine(c1,c2){
    //var long1 = -1.45723343;
    //var lat1 = 52.27519523;
    //var long2 = -1.45723343;
    //var lat2 = 52.1;
    var long1 = c1[0];
    var lat1 = c1[1];
    var long2 = c2[0];
    var lat2 = c2[1];
    
    var ψ1 = lat1 * (Math.PI / 180);
    var ψ2 = lat2 * (Math.PI/180);
    var λ1 = long1 * (Math.PI/180);
    var λ2 = long2 * (Math.PI/180);
    var Δψ = ψ2 - ψ1;
    var Δλ = λ2 - λ1;
    var R = 6371; // radius of earth (km)

    var a = (Math.sin(Δψ/2) **2) + (Math.cos(ψ1) * Math.cos(ψ2) * (Math.sin(Δλ/2) **2));
    var c = 2 * Math.atan2(Math.sqrt(a),Math.sqrt(1-a));
    var d = R * c;

    return d;
}

function myHaversinet(){
    return myHaversine([-1.45723343,52.27519523],[-1.45723343,52.1]);
}



