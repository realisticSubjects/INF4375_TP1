/**
 * @author florian baumont
 */

var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectID;
var fs = require("fs");
var xmldom = require("xmldom");



MongoClient.connect("mongodb://localhost:27017/BAUF12059008", function(err, db) {
    if (err) {
        console.log("cannot connect to DB!");
        throw err;

    }
    console.log("Connected");

    creerCollection(db);

});

function creerCollection(db) {
    db.collection('dossiers', function(err, collectionDossier) {
        if (err) {
            console.log("Cannot create the collection!");

        }
        traiterDossiers(collectionDossier, db);

    });
}

function traiterDossiers(collectionDossier, db) {
    fs.readFile("dossiers.xml", function(err, data) {
        if (err) {
            console.log("Error reading XML document");

        } else {
            // Le fichier XML est retourné sous forme d'un buffer. Nous devons le
            // transgormer en chaîne de caractères avant de l'envoyer au parser DOM.
            var domRoot = new xmldom.DOMParser().parseFromString(data.toString());

            var dossierList = domRoot.getElementsByTagName("dossier");

            if (!dossierList.length) {
                console.log("Le fichier ne contient aucun dossier.");

            } else {

                creerDossier(InsererDossier);

                function creerDossier(callback) {
                    var listeDossiers = [];

                    for (var i = 0; i < dossierList.length; i++) {
                        var dossierCourant = dossierList[i];
                        var id_patient = dossierCourant.getElementsByTagName("id")[0].textContent;
                        var nom_patient = dossierCourant.getElementsByTagName("nom")[0].textContent;
                        var prenom_patient = dossierCourant.getElementsByTagName("prenom")[0].textContent;
                        var sexe_patient = dossierCourant.getElementsByTagName("sexe")[0].textContent;
                        var dateNaissance_patient = dossierCourant.getElementsByTagName("dateNaissance")[0].textContent;
                        var grpSanguin_patient = dossierCourant.getElementsByTagName("groupeSanguin")[0].textContent;
                        var poid_patient = dossierCourant.getElementsByTagName("poidsKg")[0].textContent;
                        var taille_patient = dossierCourant.getElementsByTagName("tailleCm")[0].textContent;
                        var donOrganes_patient = dossierCourant.getElementsByTagName("donOrganes")[0].textContent;
                        var listeVisites =  [];

                        var objDossier = {
                            "id" : id_patient,
                            "nom" : nom_patient,
                            "prenom" : prenom_patient,
                            "sexe" : sexe_patient,
                            "dateNaissance" : dateNaissance_patient,
                            "groupeSanguin" : grpSanguin_patient,
                            "poidsKg" : poid_patient,
                            "tailleCm" : taille_patient,
                            "donOrganes" : donOrganes_patient,
                            "visites" : listeVisites
                        };
                        listeDossiers[i] = objDossier

                    }
                    callback(db, collectionDossier, listeDossiers);

                }

            }

        }

    });

    function InsererDossier(db, collectionDossier, listeDossiers ) {
        collectionDossier.insert(listeDossiers, {w : 1}, function(err, doc) {
            if (err) {
                console.log("Cannot add product in that collection!", err);
                throw err;

            } else {
                db.collection('professionnels', function (err, collectionProfessionnel) {
                    if (err) {
                        console.log("Cannot create the collection!");

                    }
                    traiterProfessionnels(collectionProfessionnel, db);

                });

                function traiterProfessionnels(collectionProfessionnel, db) {
                    fs.readFile("professionnels.xml", function (err, data) {
                        if (err) {
                            console.log("Error reading XML document");

                        } else {
                            var domRoot = new xmldom.DOMParser().parseFromString(data.toString());

                            var professionnelList = domRoot.getElementsByTagName("professionnel");

                            if (!professionnelList.length) {
                                console.log("Le fichier ne contient aucun dossier.");

                            } else {
                                creerProfessionnel(insererProfessionnel)

                                function creerProfessionnel(callback) {
                                    var listeProfessionnel = [];

                                    for (var i = 0; i < professionnelList.length; i++) {
                                        var professionnelCourant = professionnelList[i];
                                        var id_professionnel = professionnelCourant.getElementsByTagName("id")[0].textContent;
                                        var nom_professionnel = professionnelCourant.getElementsByTagName("nom")[0].textContent;
                                        var prenom_professionnel = professionnelCourant.getElementsByTagName("prenom")[0].textContent;
                                        var sexe_professionnel = professionnelCourant.getElementsByTagName("sexe")[0].textContent;
                                        var specialite_professionnel = professionnelCourant.getElementsByTagName("specialite")[0].textContent;
                                        var listePatient2014 = [];
                                        var nbVisiteTotal = 0;
                                        var nbPatient = 0;

                                        var objProfessionnel = {
                                            "id" : id_professionnel,
                                            "nom": nom_professionnel,
                                            "prenom": prenom_professionnel,
                                            "sexe": sexe_professionnel,
                                            "specialite": specialite_professionnel,
                                            "patientEn2014" : listePatient2014,
                                            "nbVisiteTotal" : nbVisiteTotal,
                                            "nbPatient" : nbPatient

                                        };
                                        listeProfessionnel[i] = objProfessionnel

                                    }
                                    callback(db, collectionProfessionnel, listeProfessionnel);

                                }
                            }
                        }
                    });
                }

                function insererProfessionnel(db, collectionProfessionnel, listeProfessionnel ) {
                    //console.log(objToInsert)
                    collectionProfessionnel.insert(listeProfessionnel, {w: 1}, function (err, doc) {
                        if (err) {
                            console.log("Cannot add product in that collection!", err);
                            throw err;

                        } else {
                            fs.readFile("visites.xml", function(err, data) {
                                if (err) {
                                    console.log("Error reading XML document");

                                } else {
                                    var domRoot = new xmldom.DOMParser().parseFromString(data.toString());

                                    var visiteList = domRoot.getElementsByTagName("visite");

                                    if (!visiteList.length) {
                                        console.log("Le fichier ne contient aucune visite.");

                                    } else {

                                        var index = 0;

                                        executerRequetes(index, fermerDb);

                                        function executerRequetes(index, callback) {
                                            var dejaVus = false;

                                            collectionProfessionnel.findOne({id: visiteList[index].getElementsByTagName("professionnel")[0].textContent }, function(err, med) {
                                                if(err){
                                                    throw err;
                                                    console.log("medecin introuvable")

                                                } else {
                                                    var addToDossier = {
                                                        "date" : visiteList[index].getElementsByTagName("date")[0].textContent,
                                                        "medecin" : (med.nom + " " +  med.prenom),
                                                        "specialite" :med.specialite

                                                    };

                                                    console.log("console log: findOne 1");

                                                    collectionProfessionnel.update({id: visiteList[index].getElementsByTagName("professionnel")[0].textContent}, {$inc: {nbVisiteTotal: 1}}, function(err, numAffected){
                                                        var addToProfessionnel;
                                                        if (err)
                                                            console.log(numAffected);
                                                        else {
                                                            console.log( "index: ", index);
                                                            console.log("console log: update 2");

                                                            for(var y = 0; y < index ; y++){
                                                                if(visiteList[y].getElementsByTagName("professionnel")[0].textContent === visiteList[index].getElementsByTagName("professionnel")[0].textContent){
                                                                    if(visiteList[y].getElementsByTagName("patient")[0].textContent === visiteList[index].getElementsByTagName("patient")[0].textContent){
                                                                        dejaVus = true;

                                                                        break;

                                                                    } else {
                                                                        dejaVus = false;

                                                                    }

                                                                }

                                                            }

                                                            if (dejaVus == false){
                                                                collectionProfessionnel.update({id: visiteList[index].getElementsByTagName("professionnel")[0].textContent}, {$inc: {nbPatient: 1}}, function(err, numAffected) {
                                                                    if (err)
                                                                        console.log(numAffected);
                                                                    else
                                                                        queryPartOne(queryPartTwo);
                                                                });


                                                            }else{
                                                                queryPartOne(queryPartTwo);

                                                            }

                                                            function queryPartOne(callback){
                                                                if(addToDossier.date > "2014-00-00")
                                                                {
                                                                    collectionDossier.findOne({id: visiteList[index].getElementsByTagName("patient")[0].textContent }, function(err, pat) {
                                                                        if(err){
                                                                            throw err;
                                                                            //console.log("medecin introuvable")

                                                                        } else {
                                                                            addToProfessionnel= {
                                                                                "nom" : pat.nom,
                                                                                "prenom" : pat.prenom,
                                                                                "identifiant" : pat.id


                                                                            };
                                                                            console.log("console log: findOne 3");
                                                                            console.log( "r3: ", index);

                                                                            collectionProfessionnel.update({id: visiteList[index].getElementsByTagName("professionnel")[0].textContent}, {$addToSet: {patientEn2014:addToProfessionnel}}, function(err, numAffected){
                                                                                if (err)
                                                                                    console.log(err);
                                                                                else {
                                                                                    console.log( "r4: ", index);

                                                                                    console.log("console log: update 4");
                                                                                    callback();
                                                                                }

                                                                            });

                                                                        }

                                                                    });

                                                                } else {
                                                                    callback();
                                                                }

                                                            }

                                                            function queryPartTwo(){
                                                                collectionDossier.update({id: visiteList[index].getElementsByTagName("patient")[0].textContent}, {$addToSet: {visites:addToDossier}}, function(err, numAffected){
                                                                    if (err)
                                                                        console.log(numAffected);
                                                                    else {
                                                                        console.log("console log: update 5");
                                                                        index++;

                                                                        if (index == visiteList.length) {
                                                                            callback(db);

                                                                        } else {
                                                                            console.log("----------------------");

                                                                            executerRequetes(index, fermerDb)

                                                                        }
                                                                    }

                                                                });

                                                            }

                                                        }

                                                    });

                                                }

                                            });

                                        }

                                    }

                                }

                            });

                            function fermerDb(db){
                                db.close();

                            }

                        }

                    });

                }

            }

        });

    }

}
