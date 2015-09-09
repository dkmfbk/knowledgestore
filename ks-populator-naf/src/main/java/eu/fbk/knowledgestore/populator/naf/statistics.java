package eu.fbk.knowledgestore.populator.naf;

public class statistics {

    private int entity = 0, coref = 0, timex = 0, srl = 0, objectMention = 0,
            timeMention = 0, eventMention = 0, rolewithEntity = 0, participationMention = 0,
            rolewithoutEntity = 0, factuality = 0,role=0, corefMentionEvent=0,corefMentionNotEvent=0
            , clinkMention=0,tlinkMention=0, clinkMentionDiscarded=0,tlinkMentionDiscarded=0,tlinkMentionsEnriched=0;
    
    public int getClinkMention() {
		return clinkMention;
	}



	public int getTlinkMentionsEnriched() {
		return tlinkMentionsEnriched;
	}



	public void setTlinkMentionsEnriched(int tlinkMentionsEnriched) {
		this.tlinkMentionsEnriched = tlinkMentionsEnriched;
	}



	public void setClinkMention(int clinkMention) {
		this.clinkMention = clinkMention;
	}



	public int getTlinkMention() {
		return tlinkMention;
	}



	public void setTlinkMention(int tlinkMention) {
		this.tlinkMention = tlinkMention;
	}



	public int getClinkMentionDiscarded() {
		return clinkMentionDiscarded;
	}



	public void setClinkMentionDiscarded(int clinkMentionDiscarded) {
		this.clinkMentionDiscarded = clinkMentionDiscarded;
	}



	public int getTlinkMentionDiscarded() {
		return tlinkMentionDiscarded;
	}



	public void setTlinkMentionDiscarded(int tlinkMentionDiscarded) {
		this.tlinkMentionDiscarded = tlinkMentionDiscarded;
	}



	public int getCorefMentionEvent() {
        return corefMentionEvent;
    }


    
    public void setCorefMentionEvent(int corefMentionEvent) {
        this.corefMentionEvent = corefMentionEvent;
    }


    
    public int getCorefMentionNotEvent() {
        return corefMentionNotEvent;
    }


    
    public void setCorefMentionNotEvent(int corefMentionNotEvent) {
        this.corefMentionNotEvent = corefMentionNotEvent;
    }

    private int PER = 0, LOC = 0, ORG = 0, PRO = 0, fin = 0, mix = 0, no_mapping = 0;

    public int getRole() {
        return role;
    }

    
    public void setRole(int role) {
        this.role = role;
    }

    public String getStats(){
        String statistics = "";
        statistics += "\nKS stats all("
                + (objectMention + timeMention + eventMention + participationMention)
                + "):\nObject mentions:" + (objectMention) + " , PER(" + PER + "), LOC(" + LOC
                + "), ORG(" + ORG + "), PRO(" + PRO + "), fin(" + fin + "), mix(" + mix
                + "), no-mapping(" + no_mapping + ")" + "\nTime mentions: " + timeMention
                + "\nEvent mentions: " + (eventMention)
                + "\nParticipation mentions: " + participationMention
                +"\nTlinks mentions: "+tlinkMention+" discarded("+tlinkMentionDiscarded+") enriched("+tlinkMentionsEnriched+") "
                 +"\nClinks mentions: "+clinkMention+" discarded("+clinkMentionDiscarded+")";
        statistics += ("\n\npopulator stats:\nEntity:" + entity + "\nCoreference: " + coref
                + " (event: "+getCorefMentionEvent()+" , not-event: "+getCorefMentionNotEvent()+" )\nSrl:" + srl);
       // statistics += ("\nRole:" + role);
        statistics += ("\nRole with entity: " + rolewithEntity+" (discarded: " + rolewithoutEntity + ")");
        statistics += ("\nTimex:"
                + timeMention + "\nFactuality:" + factuality + "\n");
        statistics+= "=========\n";
        return statistics;
    }
    
    public int getEntity() {
        return entity;
    }
    
    public void setEntity(int entity) {
        this.entity = entity;
    }
    
    public int getCoref() {
        return coref;
    }
    
    public void setCoref(int coref) {
        this.coref = coref;
    }
    
    public int getTimex() {
        return timex;
    }
    
    public void setTimex(int timex) {
        this.timex = timex;
    }
    
    public int getSrl() {
        return srl;
    }
    
    public void setSrl(int srl) {
        this.srl = srl;
    }
    
    public int getObjectMention() {
        return objectMention;
    }
    
    public void setObjectMention(int objectMention) {
        this.objectMention = objectMention;
    }
    
    public int getTimeMention() {
        return timeMention;
    }
    
    public void setTimeMention(int timeMention) {
        this.timeMention = timeMention;
    }
    
    public int getEventMention() {
        return eventMention;
    }
    
    public void setEventMention(int eventMention) {
        this.eventMention = eventMention;
    }
    
    public int getRolewithEntity() {
        return rolewithEntity;
    }
    
    public void setRolewithEntity(int rolewithEntity) {
        this.rolewithEntity = rolewithEntity;
    }
    
    public int getParticipationMention() {
        return participationMention;
    }
    
    public void setParticipationMention(int participationMention) {
        this.participationMention = participationMention;
    }
    
    public int getRolewithoutEntity() {
        return rolewithoutEntity;
    }
    
    public void setRolewithoutEntity(int rolewithoutEntity) {
        this.rolewithoutEntity = rolewithoutEntity;
    }
    
    public int getFactuality() {
        return factuality;
    }
    
    public void setFactuality(int factuality) {
        this.factuality = factuality;
    }
    
    public int getPER() {
        return PER;
    }
    
    public void setPER(int pER) {
        PER = pER;
    }
    
    public int getLOC() {
        return LOC;
    }
    
    public void setLOC(int lOC) {
        LOC = lOC;
    }
    
    public int getORG() {
        return ORG;
    }
    
    public void setORG(int oRG) {
        ORG = oRG;
    }
    
    public int getPRO() {
        return PRO;
    }
    
    public void setPRO(int pRO) {
        PRO = pRO;
    }
    
    public int getFin() {
        return fin;
    }
    
    public void setFin(int fin) {
        this.fin = fin;
    }
    
    public int getMix() {
        return mix;
    }
    
    public void setMix(int mix) {
        this.mix = mix;
    }
    
    public int getNo_mapping() {
        return no_mapping;
    }
    
    public void setNo_mapping(int no_mapping) {
        this.no_mapping = no_mapping;
    }

   

}
