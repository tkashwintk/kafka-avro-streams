package com.example.learnkafka.streams;

import com.example.learnkafka.FinalItinerary;
import com.example.learnkafka.Flight;
import com.example.learnkafka.Hotel;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.kstream.*;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;

import java.util.concurrent.TimeUnit;

@Component
public class AppStreams {

    private SpecificAvroSerde<Flight> flightSpecificAvroSerde;
    private SpecificAvroSerde<Hotel> hotelSpecificAvroSerde;
    private SpecificAvroSerde<FinalItinerary> finalItinerarySpecificAvroSerde;

    public AppStreams(SpecificAvroSerde<Flight> flightSpecificAvroSerde,
                      SpecificAvroSerde<Hotel> hotelSpecificAvroSerde,
                      SpecificAvroSerde<FinalItinerary> finalItinerarySpecificAvroSerde) {
        this.flightSpecificAvroSerde = flightSpecificAvroSerde;
        this.hotelSpecificAvroSerde = hotelSpecificAvroSerde;
        this.finalItinerarySpecificAvroSerde = finalItinerarySpecificAvroSerde;
    }

    @Autowired
    void buildPipeline(StreamsBuilder streamsBuilder) {
        KStream<String, Flight> flightbookings = streamsBuilder.stream("flightbookings",
                Consumed.with(Serdes.String(), flightSpecificAvroSerde));

        KStream<String, Hotel> hotelbookings = streamsBuilder.stream("hotelbookings",
                Consumed.with(Serdes.String(), hotelSpecificAvroSerde));

        KStream<String, FinalItinerary> finalItineraryKStream = flightbookings
                .outerJoin(hotelbookings, this::joinFlightsHotels,
                        JoinWindows.of(TimeUnit.SECONDS.toMillis(100)))
                .groupByKey()
                .aggregate(FinalItinerary::new,
                        (aggKey, newValue, aggValue) -> this.aggregateFinalItinerary(newValue,aggValue),
                        Materialized.as("holidaystore"))
                .toStream();
        finalItineraryKStream.to("finalitinerary",
                Produced.with(Serdes.String(),finalItinerarySpecificAvroSerde));
    }

    private FinalItinerary joinFlightsHotels(Flight flight, Hotel hotel) {
        FinalItinerary.Builder finalItineraryBuilder = FinalItinerary.newBuilder();
        if(flight != null) {
            finalItineraryBuilder.setCustomer(flight.getCustomer());
            finalItineraryBuilder.setAirline(flight.getAirline());
        }
        if (hotel != null) {
            finalItineraryBuilder.setCustomer(hotel.getCustomer());
            finalItineraryBuilder.setHotel(hotel.getHotel());
        }
        return finalItineraryBuilder.build();
    }

    private FinalItinerary aggregateFinalItinerary(FinalItinerary newValue, FinalItinerary aggValue) {
        if(newValue.getCustomer() != null){
            aggValue.put("customer",newValue.getCustomer());
        }
        if(newValue.getAirline() != null){
            aggValue.put("airline",newValue.getAirline());
        }
        if(newValue.getHotel() != null){
            aggValue.put("hotel",newValue.getHotel());
        }
        return null;
    }
}
