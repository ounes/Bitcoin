<!doctype html>
<!-- page.tpl -->
<HTML lang="fr">
  <HEAD>
    <TITLE>Bitcoin dashboard</TITLE>
    <meta charset="UTF-8">
		<link rel="stylesheet" type="text/css" href="/static/style.css">
		<link rel="icon" type="image/png" href="/static/favicon.png" />
  </HEAD>
 
  <body>
	<div class="logo"></div>
	<header>
		<h1>Tableau de bord</h1>
	</header>
	<nav>
		<!--<p>Date de début : {{datestart}}, date de fin : {{dateend}}</p>-->
		<button class="button" onclick="window.location.href='/lasthour'">Dernière heure</button>
		<button class="button" onclick="window.location.href='/lastdays/1'">Dernier jour</button>
		<button class="button" onclick="window.location.href='/lastdays/7'">Dernière semaine</button>
		<button class="button" onclick="window.location.href='/lastdays/30'">Dernier mois</button>
		<button class="button" onclick="window.location.href='/lastdays/90'">Dernier trimestre</button>
		<button class="button" onclick="window.location.href='/lastdays/180'">Dernier semestre</button>
		<button class="button" onclick="window.location.href='/lastdays/365'">Dernière année</button>
		<button class="button" onclick="window.location.href='/lastdays/1825'">5 dernières années</button>	
		<form method='post' action='/fromdatetodate'>
			Période :	
			<label for="datestart">du </label>
			<input class="date" type='date' id='datestart' name='datestart' value={{datestart}} required />
			<label for="dateend">au </label>
			<input class="date" type='date' id='dateend' name='dateend' value={{dateend}} required /> 
			<input class="button" type='submit' value="Valider" />
		</form>
	</nav>
	{{!iframe}}
  </body>
</HTML>